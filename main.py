"""
CLI entrypoint for the Delta-Neutral Funding Arbitrage Backtester.

Usage:
    python main.py                              # uses data/ defaults
    python main.py --spot data/spot_1m.parquet   # custom paths
"""

import argparse
import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Add project root to sys.path to avoid ImportError when run from outside the dir
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import polars as pl

from data_loader import load_data
from strategy import run_strategy
from analytics import compute_trade_summary, compute_portfolio_stats
from cost_model import round_trip_cost_pct
from config import (
    ENTRY_ANN_FUNDING_THRESHOLD,
    EXIT_ANN_FUNDING_THRESHOLD,
    ENTRY_PREMIUM_THRESHOLD,
    NOTIONAL_PER_TRADE,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def _ensure_data() -> tuple[str, str, str]:
    """Return paths, generating sample data if necessary."""
    spot = os.path.join(DATA_DIR, "spot_1m.parquet")
    perp = os.path.join(DATA_DIR, "perp_1m.parquet")
    funding = os.path.join(DATA_DIR, "funding_rates.parquet")

    if not all(os.path.exists(p) for p in (spot, perp, funding)):
        print("Data files not found - generating synthetic data ...")
        from generate_sample_data import main as gen_main
        gen_main()

    return spot, perp, funding


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delta-Neutral Funding Arbitrage Backtester"
    )
    parser.add_argument("--spot", default=None, help="Path to spot 1m parquet")
    parser.add_argument("--perp", default=None, help="Path to perp 1m parquet")
    parser.add_argument("--funding", default=None, help="Path to funding rates parquet")
    args = parser.parse_args()

    # ── Load data ────────────────────────────────────────────────────────
    if args.spot and args.perp and args.funding:
        spot_path, perp_path, funding_path = args.spot, args.perp, args.funding
    else:
        spot_path, perp_path, funding_path = _ensure_data()

    print("=" * 72)
    print("  DELTA-NEUTRAL FUNDING ARBITRAGE BACKTESTER")
    print("=" * 72)

    print("\n> Loading & aligning data ...")
    df = load_data(spot_path, perp_path, funding_path)
    print(f"  Aligned timeline: {df.height:,} rows "
          f"({df['timestamp'].min()} -> {df['timestamp'].max()})")

    # ── Run strategy ─────────────────────────────────────────────────────
    print("\n> Running strategy ...")
    print(f"  Entry: ann. funding > {ENTRY_ANN_FUNDING_THRESHOLD:.0%}, "
          f"premium > {ENTRY_PREMIUM_THRESHOLD:.2%}")
    print(f"  Exit:  ann. funding < {EXIT_ANN_FUNDING_THRESHOLD:.0%} "
          f"or perp at discount")
    print(f"  Round-trip cost: {round_trip_cost_pct():.2%}")

    df = run_strategy(df)

    num_trades = df.filter(pl.col("trade_id") > 0).select(
        pl.col("trade_id").n_unique()
    ).item()
    print(f"  Trades identified: {num_trades}")

    # ── Trade summary ────────────────────────────────────────────────────
    print("\n> Computing trade summaries ...")
    trade_summary = compute_trade_summary(df)

    if trade_summary.is_empty():
        print("\n  WARNING: No trades were generated. Consider loosening entry thresholds.")
        sys.exit(0)

    # Pretty-print the trade table
    print("\n" + "-" * 72)
    print("  PER-TRADE SUMMARY")
    print("-" * 72)

    display_cols = trade_summary.select(
        "trade_id",
        pl.col("entry_time").dt.strftime("%Y-%m-%d %H:%M"),
        pl.col("exit_time").dt.strftime("%Y-%m-%d %H:%M"),
        pl.col("duration_hours").round(1),
        (pl.col("gross_yield_pct") * 100).round(4).alias("yield_%"),
        (pl.col("basis_pnl_pct") * 100).round(4).alias("basis_%"),
        (pl.col("fees_pct") * 100).round(4).alias("fees_%"),
        (pl.col("net_pnl_pct") * 100).round(4).alias("net_%"),
        pl.col("net_pnl_usd").round(2).alias("net_$"),
        (pl.col("annualized_return_pct") * 100).round(1).alias("ann_ret_%"),
    )

    # Polars default print is clean enough for a terminal
    pl.Config.set_tbl_cols(15)
    pl.Config.set_tbl_width_chars(120)
    print(display_cols)

    # ── Portfolio stats ──────────────────────────────────────────────────
    stats = compute_portfolio_stats(trade_summary)

    print("\n" + "-" * 72)
    print("  PORTFOLIO SUMMARY")
    print("-" * 72)
    print(f"  Total trades        : {stats['total_trades']}")
    print(f"  Winners / Losers    : {stats['winners']} / {stats['losers']}")
    print(f"  Win rate            : {stats['win_rate']:.1%}")
    print(f"  Total net PnL       : ${stats['total_net_pnl_usd']:,.2f}  "
          f"(on ${NOTIONAL_PER_TRADE:,.0f} notional per trade)")
    print(f"  Avg net PnL         : {stats['avg_net_pnl_pct'] * 100:.4f}%")
    print(f"  Median net PnL      : {stats['median_net_pnl_pct'] * 100:.4f}%")
    print(f"  Best trade          : {stats['best_trade_pct'] * 100:.4f}%")
    print(f"  Worst trade         : {stats['worst_trade_pct'] * 100:.4f}%")
    print(f"  Avg duration        : {stats['avg_duration_hours']:.1f} hours")
    print(f"  Avg ann. return     : {stats['avg_annualized_return_pct'] * 100:.1f}%")
    print(f"  Total gross yield   : {stats['total_gross_yield_pct'] * 100:.4f}%")
    print(f"  Total fees          : {stats['total_fees_pct'] * 100:.4f}%")
    print("-" * 72)


if __name__ == "__main__":
    main()
