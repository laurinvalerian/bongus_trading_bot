"""
Realistic cost model for the delta-neutral funding arbitrage strategy.

Each "action" (open or close) involves TWO legs:
    - Spot: market buy / sell
    - Perp: market sell / buy

Each leg incurs a taker fee + slippage estimate.

A full round trip = open + close = 4 legs total.
"""

from config import (
    TAKER_FEE,
    SLIPPAGE_ESTIMATE,
    LEGS_PER_ACTION,
    ACTIONS_PER_ROUND_TRIP,
)


def cost_per_leg() -> float:
    """Fractional cost for a single leg (one side of one action)."""
    return TAKER_FEE + SLIPPAGE_ESTIMATE


def action_cost_pct() -> float:
    """Fractional cost for one action (open OR close), both legs."""
    return cost_per_leg() * LEGS_PER_ACTION


def round_trip_cost_pct() -> float:
    """Total fractional cost for a full round trip (open + close)."""
    return action_cost_pct() * ACTIONS_PER_ROUND_TRIP


def entry_cost(notional: float) -> float:
    """Dollar cost to open the hedge (spot long + perp short)."""
    return notional * action_cost_pct()


def exit_cost(notional: float) -> float:
    """Dollar cost to close the hedge (spot sell + perp buy)."""
    return notional * action_cost_pct()


def round_trip_cost(notional: float) -> float:
    """Total dollar cost for open + close."""
    return notional * round_trip_cost_pct()
