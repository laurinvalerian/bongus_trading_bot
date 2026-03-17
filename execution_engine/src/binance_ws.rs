use futures_util::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn, error};

use crate::order_manager::WsEvent;

const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws";

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerShutdownEvent {
    pub e: String, // "serverShutdown"
}

#[derive(Debug)]
pub enum WsState {
    Disconnected,
    Connecting,
    Connected,
    ShuttingDown,
}

pub struct WsConnectionManager {
    url: String,
    state: WsState,
    reconnect_delay_ms: u64,
    event_sender: Sender<WsEvent>,
}

impl WsConnectionManager {
    pub fn new(event_sender: Sender<WsEvent>) -> Self {
        Self {
            url: BINANCE_WS_URL.to_string(),
            state: WsState::Disconnected,
            reconnect_delay_ms: 1000,
            event_sender,
        }
    }

    pub async fn run(&mut self) {
        loop {
            self.state = WsState::Connecting;
            info!("Attempting to connect to {}", self.url);

            match connect_async(&self.url).await {
                Ok((ws_stream, _)) => {
                    info!("Successfully connected to Binance WebSocket.");
                    self.state = WsState::Connected;
                    let _ = self.event_sender.send(WsEvent::Connected).await;
                    self.reconnect_delay_ms = 1000; // reset backoff

                    self.handle_connection(ws_stream).await;
                }
                Err(e) => {
                    error!("Failed to connect: {}. Retrying in {}ms", e, self.reconnect_delay_ms);
                    self.state = WsState::Disconnected;
                    let _ = self.event_sender.send(WsEvent::Disconnected).await;
                }
            }

            // Exponential backoff
            sleep(Duration::from_millis(self.reconnect_delay_ms)).await;
            self.reconnect_delay_ms = std::cmp::min(self.reconnect_delay_ms * 2, 60_000);
        }
    }

    async fn handle_connection(&mut self, mut ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>) {
        // Subscribe to a dummy stream for demonstration
        let sub_req = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@aggTrade"],
            "id": 1
        });
        
        if let Err(e) = ws_stream.send(Message::Text(sub_req.to_string())).await {
            error!("Error sending subscription: {}", e);
            return;
        }

        while let Some(msg_result) = ws_stream.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    // Fast check for serverShutdown
                    if text.contains(r#""e":"serverShutdown""#) {
                        warn!("CRITICAL: Received serverShutdown event from Binance!");
                        self.state = WsState::ShuttingDown;
                        self.handle_server_shutdown(&mut ws_stream).await;
                        break; // Exit connection loop to trigger reconnect
                    }
                    
                    // Normal message processing would go here
                    // info!("Received message: {}", text);
                }
                Ok(Message::Ping(ping_data)) => {
                    // Auto-reply with Pong
                    if let Err(e) = ws_stream.send(Message::Pong(ping_data)).await {
                        error!("Failed to send Pong: {}", e);
                        break;
                    }
                }
                Ok(Message::Close(frame)) => {
                    warn!("WebSocket closed by server: {:?}", frame);
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
        
        info!("Connection loop exited. Preparing to reconnect.");
        let _ = self.event_sender.send(WsEvent::Disconnected).await;
    }

    async fn handle_server_shutdown(&mut self, ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) {
        info!("Executing emergency shutdown sequence...");
        // 1. Halt new order submissions (broadcast to other parts of system)
        // 2. Dispatch emergency cancelation requests via WS if possible
        
        let cancel_all_req = serde_json::json!({
            "method": "SERVER_SHUTDOWN_EMERGENCY_CANCEL",
            "params": []
        });

        info!("Sending emergency order cancelations...");
        let _ = ws_stream.send(Message::Text(cancel_all_req.to_string())).await;
        
        // Brief delay to allow cancelations to hopefully transmit before socket completely dies
        sleep(Duration::from_millis(100)).await;
        
        let _ = ws_stream.close(None).await;
        info!("Emergency shutdown sequence complete. Socket closed.");
    }
}
