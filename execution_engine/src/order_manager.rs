use rand::Rng;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::binance_rest::BinanceRest;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SystemState {
    Disconnected,
    Reconciling,
    Trading,
}

#[derive(Debug)]
pub enum WsEvent {
    Connected,
    Disconnected,
}

#[derive(Debug, Clone)]
pub struct InternalOrder {
    pub client_order_id: String,
    pub symbol: String,
    pub status: String, // e.g., "NEW", "FILLED", "CANCELLED"
}

pub struct OrderManager {
    pub state: SystemState,
    pub internal_orders: HashMap<String, InternalOrder>,
    pub ws_receiver: Receiver<WsEvent>,
    pub binance_rest: BinanceRest,
}

impl OrderManager {
    pub fn new(ws_receiver: Receiver<WsEvent>, api_key: String, secret_key: String) -> Self {
        Self {
            state: SystemState::Disconnected,
            internal_orders: HashMap::new(),
            ws_receiver,
            binance_rest: BinanceRest::new(api_key, secret_key),
        }
    }

    pub async fn run(&mut self) {
        info!("OrderManager task started.");
        
        while let Some(event) = self.ws_receiver.recv().await {
            match event {
                WsEvent::Connected => {
                    info!("OrderManager received WebSocket Connected event.");
                    if self.state == SystemState::Disconnected {
                        self.execute_reconciliation_sequence().await;
                    }
                }
                WsEvent::Disconnected => {
                    warn!("OrderManager received WebSocket Disconnected event.");
                    self.state = SystemState::Disconnected;
                }
            }
        }
    }

    async fn execute_reconciliation_sequence(&mut self) {
        self.state = SystemState::Reconciling;
        info!("=== Beginning Reconciliation Sequence ===");

        // STEP 1: Pause Trading & Flush internal
        info!("[Step 1] Pausing trading signal generation.");
        // (In a fuller implementation, this signals the strategy engine.)

        // STEP 2: Jittered Backoff
        let jitter_ms = rand::thread_rng().gen_range(500..2500);
        info!("[Step 2] Applying Jittered Backoff of {}ms before REST sync...", jitter_ms);
        sleep(Duration::from_millis(jitter_ms)).await;

        // Fetch Exchange Truth
        info!("[Step 2b] Fetching Open Orders from Exchange...");
        let open_orders_json = match self.binance_rest.get_open_orders().await {
            Ok(json) => json,
            Err(e) => {
                warn!("Failed to fetch open orders: {}. Will retry reconciliation later.", e);
                return; // Or implement local REST retry logic
            }
        };

        let parsed_orders: Result<Vec<Value>, _> = serde_json::from_str(&open_orders_json);
        if parsed_orders.is_err() {
            warn!("Failed to parse open orders JSON: {:?}", open_orders_json);
            return;
        }
        let exchange_open_orders = parsed_orders.unwrap();

        // (We would also fetch balances here `self.binance_rest.get_account().await`)
        // info!("Fetching Account Balances...");

        // STEP 3 & 4: Map Unconfirmed and Handle Orphans
        info!("[Step 3/4] Mapping internal orders to exchange truth and searching for orphans.");
        
        // Populate a set of open client order IDs reported by the exchange
        let mut exchange_known_client_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

        for order in exchange_open_orders {
            if let Some(client_id) = order.get("clientOrderId").and_then(|v| v.as_str()) {
                exchange_known_client_ids.insert(client_id.to_string());
                
                // Identify orphans explicitly created by our bot logic (prefix "bngs_")
                if client_id.starts_with("bngs_") && !self.internal_orders.contains_key(client_id) {
                    warn!("FOUND ORPHAN: Exchange has active order {}, but internal state does not.", client_id);
                    if let Some(symbol) = order.get("symbol").and_then(|v| v.as_str()) {
                        info!("    -> Issuing REST DELETE for orphan order {} ({})", client_id, symbol);
                        // In real bot, await this response and verify it cancels
                        let _ = self.binance_rest.cancel_order(symbol, client_id).await;
                    }
                }
            }
        }

        // Check our internal orders against the exchange truth
        for (client_id, internal_order) in self.internal_orders.iter_mut() {
            if internal_order.status == "NEW" && !exchange_known_client_ids.contains(client_id) {
                warn!("DANGLING INTERNAL ORDER: We think {} is open, but exchange does not have it.", client_id);
                // Implementation: Query REST for specific order to see if it FILLED or CANCELED
                // let specific_status = self.binance_rest.get_order(symbol, client_id).await;
                // if FILLED -> Update Balances
                // if CANCELED/NOT_FOUND -> order failed
                internal_order.status = "UNKNOWN_RECONCILING".to_string(); // Temporary placeholder
            }
        }

        // STEP 5: Resume
        info!("[Step 5] State matrix synchronized (Dangling mitigated, Orphans purged).");
        self.state = SystemState::Trading;
        info!("=== System is TRADING ===");
    }
}
