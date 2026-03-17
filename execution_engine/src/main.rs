mod binance_rest;
mod binance_ws;
mod collateral_engine;
mod order_manager;

use binance_ws::WsConnectionManager;
use order_manager::OrderManager;
use tokio::sync::mpsc;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    // Initialize standard logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    tracing::info!("Starting Binance Execution Engine (Rust)...");

    // Communication channel between WS Manager and Order Manager
    let (tx, rx) = mpsc::channel(32);

    // Initialize Order Manager
    // NOTE: In production, API keys should be loaded from env vars or secure config
    let mut order_manager = OrderManager::new(
        rx, 
        "DUMMY_API_KEY".to_string(), 
        "DUMMY_SECRET_KEY".to_string()
    );

    // Spawn the Order Manager loop on a separate task
    tokio::spawn(async move {
        order_manager.run().await;
    });

    // Initialize WebSocket Manager
    let binance_ws_url = "wss://fstream.binance.com/ws"; // default futures WS url
    let symbol = "btcusdt"; // example symbol
    let mut ws_manager = WsConnectionManager::new(binance_ws_url, symbol, tx);
    
    // Block on the connection manager loop
    ws_manager.run().await;
}
