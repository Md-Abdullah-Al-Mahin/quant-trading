"""
Load and prepare price data for backtesting.

Self-contained — no imports from research/.  Reads monthly CSV files stored as
data/<year>/PRICES_<year>-M<month>.csv (the project's default layout) and
returns a wide DataFrame (date × ticker) suitable for the backtester pipeline.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Sequence

import pandas as pd

from functions.data_loading import aggregate_long_prices, discover_monthly_csv_files
from functions.dates import parse_date
from functions.io import read_csv_or_none

DEFAULT_COLUMNS = ("date", "ticker", "open", "high", "low", "close", "volume", "adj_close")
_DATE_COL = "date"
_TICKER_COL = "ticker"
_DEFAULT_VALUE_COL = "adj_close"


def load_prices(
    data_dir: str | Path,
    tickers: Sequence[str] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    value_col: str = _DEFAULT_VALUE_COL,
    file_pattern: str = "PRICES_*.csv",
) -> pd.DataFrame:
    """Load price CSVs and return a **wide** DataFrame (date × ticker).

    Parameters
    ----------
    data_dir:
        Root directory containing year subfolders with monthly CSVs
        (e.g. ``data/2024/PRICES_2024-M01.csv``).
    tickers:
        Keep only these symbols.  ``None`` keeps all tickers found.
    start_date, end_date:
        Inclusive date bounds (``date`` or ISO-8601 string).  ``None`` = no bound.
    value_col:
        Column to pivot on (default ``adj_close``).
    file_pattern:
        Glob pattern for CSV filenames when date bounds are not used.

    Returns
    -------
    pd.DataFrame
        Index = ``DatetimeIndex`` (sorted ascending), columns = ticker symbols,
        values = *value_col*.  Missing trading days for a ticker are ``NaN``.
    """
    root = Path(data_dir)
    if not root.is_dir():
        return pd.DataFrame()

    start = parse_date(start_date)
    end = parse_date(end_date)

    files = discover_monthly_csv_files(root, start, end, file_pattern)

    parts = [
        df
        for f in files
        if (df := read_csv_or_none(f, parse_dates=[_DATE_COL])) is not None and not df.empty
    ]
    if not parts:
        return pd.DataFrame()

    long = aggregate_long_prices(
        parts,
        date_col=_DATE_COL,
        ticker_col=_TICKER_COL,
        start_date=start,
        end_date=end,
        tickers=tickers,
    )

    required = {_DATE_COL, _TICKER_COL, value_col}
    if not required.issubset(long.columns):
        missing = required - set(long.columns)
        raise ValueError(f"CSV files missing required columns: {missing}")

    if long.empty:
        return pd.DataFrame()

    wide = (
        long.pivot(index=_DATE_COL, columns=_TICKER_COL, values=value_col)
        .sort_index()
    )
    wide.index.name = _DATE_COL
    wide.columns.name = None

    return wide
