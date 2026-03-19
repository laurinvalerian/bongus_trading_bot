import os
from dotenv import load_dotenv

load_dotenv()

ENV = os.getenv("ENV", "testnet").lower()

if ENV == "mainnet":
    BINANCE_SPOT_REST_URL = "https://api.binance.com"
    BINANCE_USD_M_REST_URL = "https://fapi.binance.com"
    BINANCE_USD_M_WS_URL = "wss://fstream.binance.com/ws"
else:
    BINANCE_SPOT_REST_URL = "https://testnet.binance.vision"
    BINANCE_USD_M_REST_URL = "https://testnet.binancefuture.com"
    BINANCE_USD_M_WS_URL = "wss://stream.binancefuture.com/ws"

# ── Cost Model ────────────────────────────────────────────────────────────────
TAKER_FEE = 0.0004          # 0.04% per leg (standard Binance/Bybit retail)
MAKER_FEE = -0.00005        # -0.005% rebate for providing liquidity (VIP/Market Maker)
SLIPPAGE_ESTIMATE = 0.0002  # 0.02% per leg to account for bid-ask crossing

# Each action (open or close) touches 2 legs (spot + perp).
# A full round-trip is 2 actions × 2 legs = 4 crosses.
LEGS_PER_ACTION = 2
ACTIONS_PER_ROUND_TRIP = 2  # open + close

# ── Funding Schedule ─────────────────────────────────────────────────────────
FUNDING_INTERVAL_HOURS = 8       # Binance/Bybit default: every 8 hours
FUNDING_PERIODS_PER_DAY = 24 / FUNDING_INTERVAL_HOURS  # 3
FUNDING_PERIODS_PER_YEAR = FUNDING_PERIODS_PER_DAY * 365  # 1095

# Snapshot hours (UTC) at which funding is paid
FUNDING_SNAPSHOT_HOURS = [0, 8, 16]

# ── Entry Thresholds ─────────────────────────────────────────────────────────
ENTRY_ANN_FUNDING_THRESHOLD = 0.10   # 15% annualized threshold to enter
ENTRY_PREMIUM_THRESHOLD = 0.0006     # 0.01% perp premium over spot (more frequent trading)

# ── Exit Thresholds ──────────────────────────────────────────────────────────
EXIT_ANN_FUNDING_THRESHOLD = 0.00    # 5% annualized – too low to justify
EXIT_DISCOUNT_THRESHOLD = -0.0005        # exit if perp trades at or below spot

# ── Capital ───────────────────────────────────────────────────────────────────
# Demo Account Constraints ($10k starting capital)
NOTIONAL_PER_TRADE = 10_000   # USD notional deployed per side (1x leverage on $10k demo for safety)

# ── Data & Latency Controls ────────────────────────────────────────────────
MAX_ALLOWED_GAP_MINUTES = 1
MAX_FUNDING_STALENESS_MINUTES = 8 * 60

# ── Risk Limits ─────────────────────────────────────────────────────────────
MAX_GROSS_EXPOSURE_USD = 40_000 # Max 4 open trades (4 * 10k)
MAX_SYMBOL_CONCENTRATION = 0.50
SOFT_DRAWDOWN_PCT = 0.05
MAX_DRAWDOWN_PCT = 0.10
MAX_VENUE_LATENCY_MS = 400

# ── Research Acceptance Gates ───────────────────────────────────────────────
WF_MIN_AVG_OOS_EDGE = 0.0
WF_MIN_WINDOWS_PASSING = 2
WF_MIN_TRADES_PER_WINDOW = 10
WF_MIN_SIGNAL_TO_NOISE = 0.1
