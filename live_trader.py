import time
import requests
from execution_alpha import RustIPCBridge, OrderIntent
from config import ENTRY_ANN_FUNDING_THRESHOLD, NOTIONAL_PER_TRADE

def get_live_data(symbol="BTCUSDT"):
    try:
        # Get spot price
        spot_req = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}")
        spot_price = float(spot_req.json()["price"])
        
        # Get funding rate
        fund_req = requests.get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}")
        fund_data = fund_req.json()
        funding_rate = float(fund_data["lastFundingRate"])
        
        # Annualized funding (assuming 8h periods = 3x daily = 1095x yearly)
        ann_funding = funding_rate * 3 * 365
        return True, spot_price, ann_funding
        
    except Exception as e:
        print(f"Error fetching live data: {e}")
        return False, 0.0, 0.0

def main():
    print("Starting Live Trading Monitor...")
    print("Connecting to local Rust execution engine...")
    engine = RustIPCBridge(endpoint="tcp://127.0.0.1:5555")
    
    in_position = False

    while True:
        success, spot_price, ann_funding = get_live_data("BTCUSDT")
        
        if success:
            qty = round(NOTIONAL_PER_TRADE / spot_price, 4)
            print(f"BTCUSDT Price: ${spot_price:.2f} | Ann. Funding: {ann_funding:.2%} | Threshold: {ENTRY_ANN_FUNDING_THRESHOLD:.2%}")
            
            # Simple strategy check: If over threshold and not in position, Fire!
            if ann_funding >= ENTRY_ANN_FUNDING_THRESHOLD and not in_position:
                print(">>> SIGNAL DETECTED. Sending execution intent to Rust Engine. <<<")
                
                # To capture funding, we short perp and long spot. Our action is "ENTER_SHORT"
                # (Execution Alpha translates ENTER_SHORT to buying spot + shorting futures)
                intent = OrderIntent(
                    symbol="BTCUSDT",
                    side="SELL",  # Shorting the basis strategy
                    quantity=qty,
                    urgency=0.5,
                    max_slippage_bps=10.0
                )
                
                engine.dispatch_intent(intent)
                in_position = True
                
            elif ann_funding < (ENTRY_ANN_FUNDING_THRESHOLD / 2) and in_position:
                print(">>> EXIT SIGNAL DETECTED. Unwinding position. <<<")
                # Intent to close
                intent = OrderIntent(
                    symbol="BTCUSDT",
                    side="BUY", 
                    quantity=qty,
                    urgency=0.5,
                    max_slippage_bps=10.0
                )
                engine.dispatch_intent(intent)
                in_position = False
                
        time.sleep(10)

if __name__ == "__main__":
    main()