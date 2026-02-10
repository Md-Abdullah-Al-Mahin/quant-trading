# Research Module

Quantitative trading research and data management.

## Folder Structure

```
research/
├── config/              # Configuration files
│   ├── __init__.py
│   └── constants.py    # UNIVERSE (ticker list), START_DATE
│
├── functions/          # Core function modules
│   ├── __init__.py
│   ├── data_source.py  # Fetch prices from yfinance
│   └── load_data.py    # Load quarterly price CSVs
│
├── raw/                # Raw data files
│   ├── etfs.csv       # 186 ETF tickers with sectors/descriptions
│   └── stocks.csv     # 134 stock tickers with sectors/descriptions
│
└── notebooks/          # Jupyter notebooks
    └── download_prices.ipynb  # Download and save price data
```

## Import Examples

### From notebooks or scripts:

```python
# Configuration
from research.config.constants import UNIVERSE, START_DATE

# Data functions
from research.functions.data_source import fetch_prices
from research.functions.load_data import load_prices

# Usage
import pandas as pd
from datetime import date

# Download prices
df = fetch_prices(
    tickers=["AAPL", "MSFT"],
    start_date=date(2020, 1, 1),
    end_date=date.today()
)

# Load saved prices
df = load_prices(
    tickers=["AAPL", "MSFT"],
    start_date="2020-01-01",
    end_date="2023-12-31"
)
```

## Naming Conventions

- **Folders**: lowercase, descriptive names (config, functions, raw, notebooks)
- **Python files**: lowercase with underscores (data_source.py, load_data.py)
- **CSV files**: lowercase with underscores (etfs.csv, stocks.csv)
- **Constants**: UPPERCASE (UNIVERSE, START_DATE, PRICE_COLUMNS)

## Notes

- The `functions/` folder contains reusable Python modules
- The `raw/` folder contains reference data (ticker lists, metadata)
- The `config/` folder stores configuration and parameters
- The `notebooks/` folder is for exploratory analysis and workflows
