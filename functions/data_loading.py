"""
Shared price-data loading helpers: discover monthly CSV files and aggregate long-format DataFrames.
No dependencies on backtester or research.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Sequence

import pandas as pd

from functions.dates import months_in_range, parse_date

# Default layout: data/<year>/PRICES_<year>-M<month>.csv
DEFAULT_MONTHLY_FILENAME = "PRICES_{y}-M{m:02d}.csv"
DEFAULT_FILE_PATTERN = "PRICES_*.csv"


def discover_monthly_csv_files(
    root: Path,
    start: date | None,
    end: date | None,
    file_pattern: str = DEFAULT_FILE_PATTERN,
    filename_template: str = DEFAULT_MONTHLY_FILENAME,
) -> list[Path]:
    """List CSV paths under *root*, optionally limited to a date range.

    When both *start* and *end* are given, returns paths for each month in
    [start, end] using the layout root/<year>/<filename_template formatted with y, m>.
    Otherwise returns all paths matching *file_pattern* under *root* (e.g. rglob).
    """
    if start is not None and end is not None:
        return [
            root / str(y) / filename_template.format(y=y, m=m)
            for y, m in months_in_range(start, end)
        ]
    return sorted(root.rglob(file_pattern))


def aggregate_long_prices(
    parts: list[pd.DataFrame],
    date_col: str = "date",
    ticker_col: str = "ticker",
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    tickers: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Concatenate long-format price DataFrames, dedupe, and filter by date/tickers.

    parts : List of DataFrames with at least *date_col* and *ticker_col*.
    start_date, end_date : Inclusive bounds (date or ISO string); None = no bound.
    tickers : Keep only these tickers; None = keep all.

    Returns a single long DataFrame sorted by date then ticker.
    """
    if not parts:
        return pd.DataFrame()

    out = pd.concat(parts, ignore_index=True)
    out[date_col] = pd.to_datetime(out[date_col])
    out = out.drop_duplicates(subset=[date_col, ticker_col])

    start = parse_date(start_date)
    end = parse_date(end_date)
    if start is not None:
        out = out[out[date_col] >= pd.Timestamp(start)]
    if end is not None:
        out = out[out[date_col] <= pd.Timestamp(end)]
    if tickers is not None:
        out = out[out[ticker_col].isin(tickers)]

    return out.sort_values([date_col, ticker_col]).reset_index(drop=True)
