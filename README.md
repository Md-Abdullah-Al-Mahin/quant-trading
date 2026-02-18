# quant-trading

Quantitative trading research focused on **pairs trading**: finding cointegrated stock/ETF pairs and backtesting a mean-reversion strategy on the spread.

## What this project does

1. **Data** – Download and store daily price data (OHLCV, adj close) from Yahoo Finance. Data lives in monthly CSVs under `data/<year>/PRICES_<year>-M<month>.csv`. You can backfill missing trading days for specific tickers.
2. **Universe** – The ticker universe is defined by `research/raw/*.csv` (e.g. `stocks.csv`, `etfs.csv`) with columns like Sector, Ticker, Full Name. `research/config/constants.py` exposes `get_universe()` and `START_DATE`.
3. **Cointegration** – Run Engle–Granger cointegration tests on all pairs from the universe. Output is `research/processed/cointegration_results.csv` (ticker1, ticker2, pvalue, coint_t, critical values, cointegrated_5pct, spread_std, etc.). Pairs with low p-value and sensible spread_std are candidates for trading.
4. **Backtest** – For chosen pairs, run a z-score mean-reversion backtest: rolling OLS hedge ratio, rolling z-score of the spread, enter/exit on z thresholds, daily PnL and costs. The backtest is limited to “full history minus one year” by default. Outputs summary stats (Sharpe, total PnL, n_trades) and plots: normalized ticker prices and cumulative PnL.

## Project layout

```
quant-trading/
├── data/                          # Price data: <year>/PRICES_<year>-M<month>.csv
├── research/
│   ├── config/
│   │   └── constants.py           # get_universe(), START_DATE, UNIVERSE_DF
│   ├── functions/
│   │   ├── load_data.py           # load_prices() from monthly CSVs
│   │   ├── coint_worker.py        # Engle–Granger cointegration (parallel worker)
│   │   ├── fetch_and_store.py    # Fetch from API and write to data/
│   │   ├── download_helper.py     # Helpers for download/backfill
│   │   └── data_source.py
│   ├── raw/                       # Universe CSVs: stocks.csv, etfs.csv
│   ├── processed/
│   │   └── cointegration_results.csv
│   └── notebooks/
│       ├── download_prices.ipynb   # Batch download prices into data/
│       ├── backfill_prices.ipynb  # Backfill missing days for given tickers
│       ├── cointegration_analysis.ipynb  # All-pairs cointegration → CSV
│       └── pairs_backtest.ipynb  # Z-score strategy backtest + price/PnL plots
├── books/                         # Extra material (e.g. books/ch3)
├── requirements.txt
└── README.md
```

## Setup

- Python 3 (tested with 3.12). Create a venv, then:

```bash
pip install -r requirements.txt
```

- Run notebooks from the project root (so `research/` is on the path). Notebooks resolve the project root by walking up until `.git` is found.

## Workflow

1. **Universe** – Ensure `research/raw/stocks.csv` and/or `research/raw/etfs.csv` list the tickers you want. Adjust `START_DATE` in `research/config/constants.py` if needed.
2. **Download** – Use `research/notebooks/download_prices.ipynb` to fetch prices in batches and write monthly CSVs under `data/`. Use `backfill_prices.ipynb` to fill gaps for specific tickers.
3. **Cointegration** – Run `cointegration_analysis.ipynb` to test all pairs and write `research/processed/cointegration_results.csv`. Filter by pvalue and spread_std to pick pairs.
4. **Backtest** – In `pairs_backtest.ipynb`, set `PAIRS` (e.g. `[("BKNG", "MA")]`) and strategy params (LOOKBACK, ENTRY_Z, EXIT_Z, CAPITAL, COST_BPS). Run to get Sharpe, PnL, and plots (normalized prices + cumulative PnL).

## Dependencies (see requirements.txt)

- **pandas**, **yfinance** – data loading and download
- **numpy**, **matplotlib** – analysis and plots
- **statsmodels** – OLS and Engle–Granger cointegration
- **xlrd** – optional, for Excel in `books/`

## Strategy (pairs backtest)

- Spread: `S = price_A - β * price_B` with rolling OLS β over a lookback window.
- Z-score the spread with the same lookback.
- Enter long spread when z < −ENTRY_Z, short when z > +ENTRY_Z; exit when z crosses back through ±EXIT_Z.
- PnL: position × change in spread, scaled by capital; round-trip costs in bps applied on position changes.
