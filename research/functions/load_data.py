"""
Load monthly price CSVs into a single DataFrame. Optional filters: tickers, start_date, end_date.
"""

from datetime import date
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume", "adj_close"]


def _parse_date(x: date | str | None) -> date | None:
    if x is None:
        return None
    return date.fromisoformat(x) if isinstance(x, str) else x


def _months_in_range(start: date, end: date):
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        yield y, m
        m, y = (m + 1, y) if m < 12 else (1, y + 1)


def _read_csv(path: Path) -> pd.DataFrame | None:
    try:
        return pd.read_csv(path, parse_dates=["date"]) if path.is_file() else None
    except Exception:
        return None


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

    start = _parse_date(start_date)
    end = _parse_date(end_date)

    if start is not None or end is not None:
        start = start or date(1900, 1, 1)
        end = end or date(2100, 12, 31)
        files = [root / str(y) / f"PRICES_{y}-M{m:02d}.csv" for y, m in _months_in_range(start, end)]
    else:
        files = sorted(root.rglob("PRICES_*.csv"))

    parts = [df for f in files if (df := _read_csv(f)) is not None and not df.empty]
    if not parts:
        return pd.DataFrame(columns=COLUMNS)

    out = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["date", "ticker"])
    dt = pd.to_datetime(out["date"]).dt.date
    lo, hi = start or date(1900, 1, 1), end or date(2100, 12, 31)
    out = out[(dt >= lo) & (dt <= hi)]
    if tickers:
        out = out[out["ticker"].isin(tickers)]
    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)
    if columns:
        out = out[columns]
    return out
