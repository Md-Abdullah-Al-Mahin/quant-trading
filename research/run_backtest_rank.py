"""
Run z-score mean-reversion backtests on all cointegrated pairs (or a capped subset),
then print the top 10 pairs by Sharpe ratio.

Usage (from project root):
  python -m research.run_backtest_rank

Optional env or edit below: MAX_PAIRS (cap number of pairs to backtest), TOP_N (number to return).
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd

# Project root
ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = ROOT / "research" / "processed"
DATA_DIR = ROOT / "data"

# Cointegration results (5% level, non-collinear)
COINT_CSV = PROCESSED_DIR / "cointegration_results_5pct_noncollinear.csv"

# Backtest params (match pairs_backtest.ipynb)
LOOKBACK = 60
ENTRY_Z = 2.0
EXIT_Z = 0.0
CAPITAL = 100_000
COST_BPS = 5
# Require at least this many position changes (~half are round-trips); filters one-trade flukes
MIN_TRADES = 4

# How many pairs to run (None = all ~72k; 10000 gives a few minutes)
MAX_PAIRS = 10_000
# How many to return
TOP_N = 10


def main():
    import sys
    sys.path.insert(0, str(ROOT))
    from research.functions.load_data import load_prices
    from research.functions.backtest_pair import backtest_pair

    print("Loading cointegration results...")
    coint = pd.read_csv(COINT_CSV)
    coint = coint.sort_values("pvalue", ascending=True).reset_index(drop=True)
    if MAX_PAIRS is not None:
        coint = coint.head(MAX_PAIRS)
    pairs_list = list(coint[["ticker1", "ticker2"]].itertuples(index=False, name=None))
    all_tickers = list({t for p in pairs_list for t in p})
    print(f"Backtesting {len(pairs_list):,} pairs ({len(all_tickers)} unique tickers)...")

    print("Loading price data...")
    prices = load_prices(
        tickers=all_tickers,
        data_dir=DATA_DIR,
        columns=["date", "ticker", "adj_close"],
    )
    prices["date"] = pd.to_datetime(prices["date"]).dt.date
    wide = prices.pivot(index="date", columns="ticker", values="adj_close").sort_index()

    max_date = wide.index.max()
    end_date = max_date - timedelta(days=365)
    wide = wide.loc[wide.index <= end_date]
    print(f"Backtest range: {wide.index.min()} to {wide.index.max()} ({len(wide)} days)")

    results = []
    for i, (a, b) in enumerate(pairs_list):
        if (i + 1) % 5000 == 0 or i == 0:
            print(f"  {i + 1:,} / {len(pairs_list):,} pairs...")
        res = backtest_pair(wide, a, b, LOOKBACK, ENTRY_Z, EXIT_Z, CAPITAL, COST_BPS)
        if "error" in res:
            continue
        results.append({k: v for k, v in res.items() if k != "pnl_series"})

    summary = pd.DataFrame(results)
    if summary.empty:
        print("No valid backtest results.")
        return

    summary = summary[summary["n_trades"] >= MIN_TRADES].copy()
    print(f"Pairs with at least {MIN_TRADES} trades: {len(summary):,}")
    summary = summary.sort_values(["sharpe", "n_trades"], ascending=[False, False]).reset_index(drop=True)
    top = summary.head(TOP_N)

    print(f"\nTop {TOP_N} pairs by Sharpe ratio (then n_trades):\n")
    print(top.to_string(index=False))

    pairs_out = [(r["ticker_a"], r["ticker_b"]) for _, r in top.iterrows()]
    print("\n# PAIRS for pairs_backtest.ipynb:")
    print("PAIRS =", pairs_out)


if __name__ == "__main__":
    main()
