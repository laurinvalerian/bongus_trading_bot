import time
import requests
import socket
import sys
import urllib3
import json
import os
from execution_alpha import RustIPCBridge, OrderIntent
from config import ENTRY_ANN_FUNDING_THRESHOLD as DEFAULT_ENTRY_ANN, NOTIONAL_PER_TRADE
import config

# Disable SSL warnings for the local testnet run
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def load_optimal_params():
    """Loads dynamically optimized parameters if available, else falls back to config.py defaults."""
    if os.path.exists("optimal_params.json"):
        try:
            with open("optimal_params.json", "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to read optimal_params.json: {e}")
    return {}

def enforce_single_instance():
    global _lock_socket
    _lock_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        _lock_socket.bind(('127.0.0.1', 49152))
    except socket.error:
        print("Another instance of live_trader is already running. Exiting.")
        sys.exit(1)

def get_live_data(symbol="BTCUSDT"):
    try:
        # Get spot price
        spot_req = requests.get(f"{config.BINANCE_SPOT_REST_URL}/api/v3/ticker/price?symbol={symbol}", verify=False)
        spot_price = float(spot_req.json()["price"])
        
        # Get funding rate
        fund_req = requests.get(f"{config.BINANCE_USD_M_REST_URL}/fapi/v1/premiumIndex?symbol={symbol}", verify=False)
        fund_data = fund_req.json()
        funding_rate = float(fund_data["lastFundingRate"])
        
        # Annualized funding (assuming 8h periods = 3x daily = 1095x yearly)
        ann_funding = funding_rate * 3 * 365
        return True, spot_price, ann_funding
        
    except Exception as e:
        print(f"Error fetching live data: {e}")
        return False, 0.0, 0.0

def main():
    enforce_single_instance()
    print("Starting Live Trading Monitor...")
    print("Connecting to local Rust execution engine...")
    engine = RustIPCBridge(endpoint="tcp://127.0.0.1:5555")
    
    in_position = False

    while True:
        # 1. Dynamically Load Best Parameters for this minute
        optimal = load_optimal_params()
        dynamic_entry = optimal.get("ENTRY_ANN_FUNDING_THRESHOLD", DEFAULT_ENTRY_ANN)
        dynamic_exit = optimal.get("EXIT_ANN_FUNDING_THRESHOLD", DEFAULT_ENTRY_ANN / 2.0)
        dynamic_slippage = optimal.get("SLIPPAGE_ESTIMATE", 10.0) # BPS

        success, spot_price, ann_funding = get_live_data("BTCUSDT")

        if success:
            qty = round(NOTIONAL_PER_TRADE / spot_price, 4)
            print(f"BTCUSDT Price: ${spot_price:.2f} | Ann. Funding: {ann_funding:.2%} | Action Threshold: {dynamic_entry:.2%}")

            # Simple strategy check: If over threshold and not in position, Fire!
            if ann_funding >= dynamic_entry and not in_position:
                print(">>> SIGNAL DETECTED. Sending execution intent to Rust Engine. <<<")

                # To capture funding, we short perp and long spot. Our action is "ENTER_SHORT"
                # (Execution Alpha translates ENTER_SHORT to buying spot + shorting futures)
                intent = OrderIntent(
                    symbol="BTCUSDT",
                    side="SELL",  # Shorting the basis strategy
                    quantity=qty,
                    urgency=0.5,
                    max_slippage_bps=dynamic_slippage * 10000 if dynamic_slippage < 1 else dynamic_slippage # Convert back to bps just in case
                )

                engine.dispatch_intent(intent)
                in_position = True

            elif ann_funding < dynamic_exit and in_position:
                print(">>> EXIT SIGNAL DETECTED. Unwinding position. <<<")
                # Intent to close
                intent = OrderIntent(
                    symbol="BTCUSDT",
                    side="BUY",
                    quantity=qty,
                    urgency=0.5,
                    max_slippage_bps=dynamic_slippage * 10000 if dynamic_slippage < 1 else dynamic_slippage
                )
                engine.dispatch_intent(intent)
                in_position = False

        time.sleep(10)

if __name__ == '__main__':
    main()