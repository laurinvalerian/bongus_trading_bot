"""
Central configuration for the Delta-Neutral Funding Arbitrage Backtester.
All tunable parameters live here so you can tweak them in one place.
"""

# ── Cost Model ────────────────────────────────────────────────────────────────
TAKER_FEE = 0.0004          # 0.04% per leg (standard Binance/Bybit retail)
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
ENTRY_ANN_FUNDING_THRESHOLD = 0.80   # 80% annualized funding rate
ENTRY_PREMIUM_THRESHOLD = 0.001      # 0.1% perp premium over spot

# ── Exit Thresholds ──────────────────────────────────────────────────────────
EXIT_ANN_FUNDING_THRESHOLD = 0.10    # 10% annualized – too low to justify
EXIT_DISCOUNT_THRESHOLD = 0.0        # exit if perp trades at or below spot

# ── Capital ───────────────────────────────────────────────────────────────────
NOTIONAL_PER_TRADE = 10_000  # USD notional deployed per side
