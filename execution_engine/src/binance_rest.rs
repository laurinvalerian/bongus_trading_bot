use hmac::{Hmac, Mac};
use reqwest::{Client, Method, RequestBuilder};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

pub struct BinanceRest {
    client: Client,
    api_key: String,
    secret_key: String,
    base_url: String,
}

impl BinanceRest {
    pub fn new(api_key: String, secret_key: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            secret_key,
            base_url: "https://api.binance.com".to_string(), // Can be parameterised
        }
    }

    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64
    }

    fn sign(&self, query_string: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.secret_key.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(query_string.as_bytes());
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    pub fn build_signed_request(
        &self,
        method: Method,
        endpoint: &str,
        mut params: Vec<(&str, String)>,
    ) -> RequestBuilder {
        params.push(("timestamp", Self::current_timestamp().to_string()));
        // Note: In real production, encode URI appropriately. This is simplified.
        let query_string = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<String>>()
            .join("&");

        let signature = self.sign(&query_string);
        let final_query = format!("{}&signature={}", query_string, signature);
        let url = format!("{}{}?{}", self.base_url, endpoint, final_query);

        self.client
            .request(method, &url)
            .header("X-MBX-APIKEY", &self.api_key)
    }

    pub async fn get_open_orders(&self) -> Result<String, reqwest::Error> {
        let req = self.build_signed_request(Method::GET, "/api/v3/openOrders", vec![]);
        req.send().await?.text().await
    }

    pub async fn get_account(&self) -> Result<String, reqwest::Error> {
        let req = self.build_signed_request(Method::GET, "/api/v3/account", vec![]);
        req.send().await?.text().await
    }

    pub async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<String, reqwest::Error> {
        let params = vec![
            ("symbol", symbol.to_string()),
            ("origClientOrderId", order_id.to_string()),
        ];
        let req = self.build_signed_request(Method::DELETE, "/api/v3/order", params);
        req.send().await?.text().await
    }
}
