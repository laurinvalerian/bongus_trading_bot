mod binance_rest;
mod binance_ws;
mod collateral_engine;
mod order_manager;
mod user_data_ws;
mod ipc;

use binance_ws::WsConnectionManager;
use user_data_ws::UserDataWsManager;
use binance_rest::BinanceRest;
use order_manager::{OrderManager, EngineEvent};
use tokio::sync::mpsc;
use tokio::sync::broadcast;
use tracing_subscriber::FmtSubscriber;
use tokio::net::TcpListener;
use tokio::io::AsyncWriteExt;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    // Attempt to load .env from the root directory
    let root_env = std::env::current_dir().unwrap().join("../.env");
    let result = dotenvy::from_path(&root_env);
    tracing::info!("Loaded .env from {:?}: {:?}", root_env, result.is_ok());

    let _ = dotenvy::dotenv(); // Also try default locations

    tracing::info!("Starting Binance Execution Engine (Rust)...");

    // Channels for primary execution
    let (engine_tx, engine_rx) = mpsc::channel(10000);
    
    // Bridge WS Events -> Engine Events
    let (ws_tx, mut ws_rx) = mpsc::channel(10000);
    let engine_tx_for_ws = engine_tx.clone();
    tokio::spawn(async move {
        while let Some(evt) = ws_rx.recv().await {
            let _ = engine_tx_for_ws.send(EngineEvent::Ws(evt)).await;
        }
    });

    // Bridge Alpha IPC -> Engine Events
    let (alpha_tx, mut alpha_rx) = mpsc::channel(10000);
    let engine_tx_for_alpha = engine_tx.clone();
    tokio::spawn(async move {
        while let Some(evt) = alpha_rx.recv().await {
            let _ = engine_tx_for_alpha.send(EngineEvent::Alpha(evt)).await;
        }
    });

    // Broadcast channel for Python Dashboard IPC
    let (dash_tx, _) = broadcast::channel(10000);

    // Load API keys securely from env
    let api_key = std::env::var("BINANCE_API_KEY").unwrap_or_else(|_| "".to_string());
    let secret_key = std::env::var("BINANCE_SECRET_KEY").unwrap_or_else(|_| std::env::var("BINANCE_API_SECRET").unwrap_or_else(|_| "".to_string()));

    if api_key.is_empty() {
        tracing::warn!("BINANCE_API_KEY is missing or empty. Please check your .env file.");
    }

    let mut order_manager = OrderManager::new(
        engine_rx,
        engine_tx,
        api_key.clone(),
        secret_key.clone(),
        dash_tx.clone()
    );

    // Spawn Order Manager
    tokio::spawn(async move {
        order_manager.run().await;
    });

    // Spawn ZeroMQ IPC Server using Unix Domain Sockets for lower latency
    let zmq_endpoint = "tcp://127.0.0.1:5555";
    let mut ipc_server = ipc::IpcServer::new(zmq_endpoint, alpha_tx);
    tokio::spawn(async move {
        ipc_server.run().await;
    });

    // Spawn User Data WebSocket Manager
    let user_data_rest_client = BinanceRest::new(
        api_key,
        secret_key,
    );
    let ud_tx = ws_tx.clone();
    tokio::spawn(async move {
        let mut ud_ws_manager = UserDataWsManager::new(user_data_rest_client, ud_tx);
        ud_ws_manager.run().await;
    });

    let top_assets = vec![
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
        "TRXUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT",
        "BCHUSDT", "UNIUSDT", "NEARUSDT", "APTUSDT", "XLMUSDT", "ATOMUSDT", "ARBUSDT"
    ];

    let binance_ws_url = std::env::var("BINANCE_USD_M_WS_URL")
        .unwrap_or_else(|_| "wss://stream.binancefuture.com/ws".to_string());

    // Spawn WsConnectionManager for each asset
    for symbol in top_assets {
        let sym = symbol.to_string();
        let tx_clone = ws_tx.clone();
        let url = binance_ws_url.to_string();
        tokio::spawn(async move {
            let mut ws_manager = WsConnectionManager::new(&url, &sym, tx_clone);
            ws_manager.run().await;
        });
        // Pace connection initialization to avoid rate limits
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Spawn IPC Server
    let dash_tx_ipc = dash_tx.clone();
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:9000").await.unwrap();
        tracing::info!("Dashboard IPC Server listening on 127.0.0.1:9000");

        while let Ok((mut socket, _)) = listener.accept().await {
            let mut rx = dash_tx_ipc.subscribe();
            tokio::spawn(async move {
                while let Ok(msg) = rx.recv().await {
                    let _ = socket.write_all(format!("{}\n", msg).as_bytes()).await;
                }
            });
        }
    });

    // Keep main thread alive
    tokio::signal::ctrl_c().await.unwrap();
    tracing::info!("Shutting down engine.");
}
