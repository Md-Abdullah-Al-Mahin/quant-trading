"""
Load monthly price CSVs into a single DataFrame. Optional filters: tickers, start_date, end_date.
"""

from datetime import date
from pathlib import Path

import pandas as pd

from functions.data_loading import aggregate_long_prices, discover_monthly_csv_files
from functions.dates import parse_date
from functions.io import read_csv_or_none

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume", "adj_close"]


def load_prices(
    tickers: list[str] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    data_dir: Path | str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Load price data from data/<year>/PRICES_<year>-M<month>.csv into one DataFrame.

    tickers: include only these symbols; None = all.
    start_date / end_date: date or "YYYY-MM-DD"; None = no bound.
    data_dir: folder containing year subfolders; None = project/data.
    columns: return only these columns; None = all columns.
    """
    root = Path(data_dir) if data_dir is not None else DATA_DIR
    if not root.is_dir():
        return pd.DataFrame(columns=COLUMNS)

    start = parse_date(start_date)
    end = parse_date(end_date)

    if start is not None or end is not None:
        start_d = start or date(1900, 1, 1)
        end_d = end or date(2100, 12, 31)
        files = discover_monthly_csv_files(root, start_d, end_d)
    else:
        files = discover_monthly_csv_files(root, None, None)

    parts = [
        df
        for f in files
        if (df := read_csv_or_none(f, parse_dates=["date"])) is not None and not df.empty
    ]
    if not parts:
        return pd.DataFrame(columns=COLUMNS)

    out = aggregate_long_prices(
        parts,
        date_col="date",
        ticker_col="ticker",
        start_date=start,
        end_date=end,
        tickers=tickers,
    )
    if columns:
        out = out[columns]
    return out
