import itertools
import polars as pl
import json
from rich.console import Console
from rich.table import Table

import data_loader
import strategy
import config
from analytics import compute_trade_summary, compute_portfolio_stats

console = Console()

def run_continual_optimizer():
    console.print("[bold cyan]Loading recent historical data for optimization...[/bold cyan]")
    try:
        # Ideally, this dataframe represents the last 14-30 days of data
        df = data_loader.load_data(
            "data/spot_1m.parquet",
            "data/perp_1m.parquet",
            "data/funding_rates.parquet",
        )
    except FileNotFoundError:
        console.print("[bold red]Data files not found. Run generate_sample_data.py or get_binance_data.py first![/bold red]")
        return

    # --- EXTENDED PARAMETER GRID TO TEST ---
    # We test every combination of these items to find the global maximum Sharpe Ratio.
    param_grid = {
        "ENTRY_ANN_FUNDING_THRESHOLD": [0.08, 0.12, 0.15, 0.20],  
        "EXIT_ANN_FUNDING_THRESHOLD": [0.00, 0.02, 0.04, 0.06],
        "ENTRY_PREMIUM_THRESHOLD": [0.0002, 0.0006, 0.0010],
        "MAX_SYMBOL_CONCENTRATION": [0.10, 0.25, 0.50],
        "SLIPPAGE_ESTIMATE": [0.0002, 0.0005, 0.0010],
        "MAX_DRAWDOWN_PCT": [0.05, 0.10]
    }

    keys, values = zip(*param_grid.items())
    combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]

    console.print(f"[bold yellow]Testing {len(combinations)} parameter setups on recent data...[/bold yellow]")
    results = []
    total_runs = len(combinations)

    for idx, params in enumerate(combinations):
        if idx % 100 == 0:
            console.print(f"Evaluating combination {idx}/{total_runs}...")
            
        # 1. Inject params into strategy/config
        strategy.ENTRY_ANN_FUNDING_THRESHOLD = params["ENTRY_ANN_FUNDING_THRESHOLD"]
        strategy.EXIT_ANN_FUNDING_THRESHOLD = params["EXIT_ANN_FUNDING_THRESHOLD"]
        strategy.ENTRY_PREMIUM_THRESHOLD = params["ENTRY_PREMIUM_THRESHOLD"]
        
        config.MAX_SYMBOL_CONCENTRATION = params["MAX_SYMBOL_CONCENTRATION"]
        config.SLIPPAGE_ESTIMATE = params["SLIPPAGE_ESTIMATE"]
        config.MAX_DRAWDOWN_PCT = params["MAX_DRAWDOWN_PCT"]

        # 2. Run Backtest
        try:
            df_sim = strategy.generate_signals(df.clone())
            df_sim = strategy.simulate_portfolio(df_sim)

            # 3. Calculate Stats
            portfolio_stats = compute_portfolio_stats(df_sim)
            
            # If we were profitable, log the results
            if portfolio_stats["total_return_pct"] > 0:
                results.append({
                    **params,
                    "return_pct": portfolio_stats["total_return_pct"],
                    "sharpe_ratio": portfolio_stats.get("sharpe_ratio", 0),
                    "max_dd_hit": portfolio_stats.get("max_drawdown_pct", 0)
                })
        except Exception as e:
            # Skip iterations where parameters caused invalid scenarios (like dividing by 0)
            continue

    # Sort results by Sharpe Ratio (Risk-Adjusted Return) descending
    results = sorted(results, key=lambda x: x["sharpe_ratio"], reverse=True)

    if not results:
        console.print("[bold red]No profitable combinations found in this market regime![/bold red]")
        return

    # Grab the #1 best performing parameter set
    best = results[0]
    
    console.print("\n[bold green]=== OPTIMIZATION COMPLETE ==-[/bold green]")
    console.print(f"Best Entry Threshold:    [bold cyan]{best['ENTRY_ANN_FUNDING_THRESHOLD']*100:.2f}%[/bold cyan]")
    console.print(f"Best Exit Threshold:     [bold cyan]{best['EXIT_ANN_FUNDING_THRESHOLD']*100:.2f}%[/bold cyan]")
    console.print(f"Best Entry Premium:      [bold cyan]{best['ENTRY_PREMIUM_THRESHOLD']*100:.3f}%[/bold cyan]")
    console.print(f"Best Capital Concen.:    [bold cyan]{best['MAX_SYMBOL_CONCENTRATION']*100:.0f}%[/bold cyan]")
    console.print(f"Optimal Est. Slippage:   [bold cyan]{best['SLIPPAGE_ESTIMATE']*100:.3f}%[/bold cyan]")
    console.print(f"Best Stop-Loss (Max DD): [bold cyan]{best['MAX_DRAWDOWN_PCT']*100:.2f}%[/bold cyan]")
    console.print("---")
    console.print(f"Expected Return:         [green]{best['return_pct']*100:.2f}%[/green]")
    console.print(f"Sharpe Ratio:            [yellow]{best['sharpe_ratio']:.2f}[/yellow]")

    # 4. Save the optimal configuration to be read by live_trader.py
    with open("optimal_params.json", "w") as f:
        json.dump(best, f, indent=4)
        
    console.print("\n[bold green]✓ Best parameters successfully saved to optimal_params.json[/bold green]")
    console.print("Have live_trader.py load this JSON on startup to auto-adapt to these new conditions!")

if __name__ == "__main__":
    run_continual_optimizer()