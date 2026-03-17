//! A dedicated, pure-math engine for simulating Binance USDⓈ-M Futures margin locally.
//! Uses the Mark Price to continuously calculate the Margin Ratio and pre-emptively
//! execute defensive maneuvers (like Collateral Injection) before liquidation occurs.

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: String,
    pub side: PositionSide,
    pub entry_price: f64,
    pub quantity: f64,
    pub leverage: u32,
}

impl Position {
    /// Calculate current Notional Value based on Mark Price
    pub fn notional_value(&self, mark_price: f64) -> f64 {
        mark_price * self.quantity
    }

    /// Calculate Unrealized PnL based on Mark Price
    pub fn unrealized_pnl(&self, mark_price: f64) -> f64 {
        match self.side {
            PositionSide::Long => (mark_price - self.entry_price) * self.quantity,
            PositionSide::Short => (self.entry_price - mark_price) * self.quantity,
        }
    }
}

pub struct MarginCalculator {
    /// Available USDT balance in the futures wallet NOT strictly locked as margin for open orders. 
    /// For simplistic tracking, this is the total wallet balance prior to PnL.
    pub cross_wallet_balance: f64,
    
    /// The specific tier-based Maintenance Margin Rate for the traded pair (e.g., 0.004 for 0.4%)
    pub maintenance_margin_rate: f64,
    
    /// The flat amount subtracted from the tier bracket calculation
    pub maintenance_amount: f64,

    /// Danger threshold. If margin ratio hits this (e.g., 0.8), trigger intervention.
    pub danger_threshold: f64,
}

impl MarginCalculator {
    pub fn new(cross_wallet_balance: f64, mmr: f64, mm_amount: f64, danger_threshold: f64) -> Self {
        Self {
            cross_wallet_balance,
            maintenance_margin_rate: mmr,
            maintenance_amount: mm_amount,
            danger_threshold,
        }
    }

    /// Update the wallet balance, for instance, after a Collateral Injection.
    pub fn inject_collateral(&mut self, amount: f64) {
        self.cross_wallet_balance += amount;
    }

    /// Computes the exact Margin Ratio given a current position and Mark Price
    pub fn calculate_margin_ratio(&self, position: &Position, mark_price: f64) -> f64 {
        let upnl = position.unrealized_pnl(mark_price);
        
        // Margin Balance = Wallet Balance + Unrealized PNL
        let margin_balance = self.cross_wallet_balance + upnl;

        if margin_balance <= 0.0 {
            // Already technically liquidated or worse
            return f64::INFINITY; 
        }

        let notional = position.notional_value(mark_price);
        
        // Maintenance Margin = Position Notional * Maintenance Margin Rate - Maintenance Amount
        let maintenance_margin = (notional * self.maintenance_margin_rate) - self.maintenance_amount;

        // Margin Ratio = Maintenance Margin / Margin Balance
        maintenance_margin / margin_balance
    }

    /// Checks if a defensive maneuver is required based on the current Mark Price
    pub fn requires_defense(&self, position: &Position, mark_price: f64) -> bool {
        let ratio = self.calculate_margin_ratio(position, mark_price);
        ratio >= self.danger_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Setup a standard Test scenario based on typical Binance $10k notional parameters
    fn setup_test_engine() -> (MarginCalculator, Position) {
        // Assume $1000 USDT in the futures wallet
        let calc = MarginCalculator::new(
            1000.0,  
            0.004,   // 0.4% MMR (typical low-tier rate for BTCUSDT)
            0.0,     // 0 maintenance amount for lowest tier
            0.8      // 80% danger threshold
        );

        // Being short 0.1 BTC from $100,000 (Notional: $10,000)
        let pos = Position {
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Short,
            entry_price: 100_000.0,
            quantity: 0.1,
            leverage: 10,
        };

        (calc, pos)
    }

    #[test]
    fn test_initial_safe_margin_ratio() {
        let (calc, pos) = setup_test_engine();
        // At entry, Mark Price == Entry Price
        let ratio = calc.calculate_margin_ratio(&pos, 100_000.0);
        
        // MM = (10,000 * 0.004) - 0 = 40
        // MB = 1000 + 0 = 1000
        // Ratio = 40 / 1000 = 0.04 (4%)
        assert!((ratio - 0.04).abs() < 1e-6);
        assert!(!calc.requires_defense(&pos, 100_000.0));
    }

    #[test]
    fn test_margin_ratio_during_pump() {
        let (calc, pos) = setup_test_engine();
        
        // Price pumps to $105,000 (Short is taking a loss of $500)
        let mark_price = 105_000.0;
        let ratio = calc.calculate_margin_ratio(&pos, mark_price);
        
        // Notional = 105,000 * 0.1 = 10,500
        // MM = 10,500 * 0.004 = 42
        // UPNL = (100k - 105k) * 0.1 = -500
        // Margin Balance = 1000 - 500 = 500
        // Ratio = 42 / 500 = 0.084 (8.4%)
        assert!((ratio - 0.084).abs() < 1e-6);
    }

    #[test]
    fn test_danger_zone_trigger() {
        let (calc, pos) = setup_test_engine();
        
        // Price pumps violently to $109,500 (Short loss is -$950)
        let mark_price = 109_500.0;
        let ratio = calc.calculate_margin_ratio(&pos, mark_price);
        
        // Notional = 10,950
        // MM = 10,950 * 0.004 = 43.8
        // UPNL = -950
        // Margin Balance = 1000 - 950 = 50
        // Ratio = 43.8 / 50 = 0.876 (87.6%) -> DANGER!
        
        assert!(ratio >= 0.80);
        assert!(calc.requires_defense(&pos, mark_price));
    }

    #[test]
    fn test_collateral_injection_recovery() {
        let (mut calc, pos) = setup_test_engine();
        let mark_price = 109_500.0;
        
        // Initial Danger
        assert!(calc.requires_defense(&pos, mark_price));
        
        // The bot automatically injects $200 USDT from the Spot wallet via REST 
        calc.inject_collateral(200.0);
        
        // New Margin Balance = 1000 (initial) - 950 (UPNL) + 200 (injected) = 250
        // MM = 43.8
        // New Ratio = 43.8 / 250 = 0.1752 (17.52%)
        
        let new_ratio = calc.calculate_margin_ratio(&pos, mark_price);
        assert!((new_ratio - 0.1752).abs() < 1e-6);
        
        // System is successfully recovered and out of the danger zone
        assert!(!calc.requires_defense(&pos, mark_price));
    }
}
