"""
Helper for downloading and managing monthly price CSV files.
Exposes date/path utilities and download orchestration with simple log output.
"""

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

import pandas as pd


@dataclass
class DownloadStats:
    """Counts for download run."""
    updated: int = 0
    up_to_date: int = 0
    no_trading_days: int = 0

    def log_summary(self) -> str:
        """Single-line summary."""
        return f"Updated: {self.updated} | Up to date: {self.up_to_date} | No new data: {self.no_trading_days}"


def month_range(year: int, month: int) -> tuple[date, date]:
    """First and last day of the given month."""
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def months_from(start: date, end: date):
    """Yield (year, month) from start through end."""
    current = date(start.year, start.month, 1)
    end_first = date(end.year, end.month, 1)
    while current <= end_first:
        yield current.year, current.month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure 'date' column is date (not datetime)."""
    if "date" not in df.columns:
        return df
    if pd.api.types.is_datetime64_any_dtype(df["date"]):
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def find_last_month_with_data(data_dir: Path) -> date | None:
    """Earliest month to resume from (from latest existing PRICES_*.csv)."""
    data_dir = Path(data_dir)
    if not data_dir.exists():
        return None
    files = sorted(data_dir.rglob("PRICES_*.csv"), reverse=True)
    if not files:
        return None
    stem = files[0].stem  # PRICES_YYYY-M##
    try:
        parts = stem.replace("_", "-").split("-")
        year, month = int(parts[1]), int(parts[2][1:])
        return date(year, month, 1)
    except (IndexError, ValueError):
        return None


def get_month_path(data_dir: Path, year: int, month: int) -> Path:
    """Path for PRICES_YYYY-M##.csv; ensures parent dir exists."""
    data_dir = Path(data_dir)
    path = data_dir / str(year) / f"PRICES_{year}-M{month:02d}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def load_existing(path: Path) -> pd.DataFrame:
    """Load CSV and normalize dates."""
    df = pd.read_csv(path, parse_dates=["date"])
    return normalize_dates(df)


def save_price_data(df: pd.DataFrame, path: Path) -> None:
    """Dedupe by (date, ticker), sort, and write CSV."""
    out = (
        df.drop_duplicates(subset=["date", "ticker"])
        .sort_values(["date", "ticker"])
        .reset_index(drop=True)
    )
    out.to_csv(path, index=False)


def update_existing_file(
    path: Path,
    end_cap: date,
    tickers: list[str],
    fetch_prices_fn: Callable[[list[str], date, date], pd.DataFrame],
    stats: DownloadStats,
) -> tuple[bool, int]:
    """Append new data to existing file. Return (updated, rows_added)."""
    existing = load_existing(path)
    last_date = existing["date"].max()
    fetch_start = last_date + timedelta(days=1)
    if fetch_start > end_cap:
        stats.up_to_date += 1
        return False, 0
    new_data = fetch_prices_fn(tickers, fetch_start, end_cap)
    if new_data.empty:
        stats.no_trading_days += 1
        return False, 0
    combined = pd.concat([existing, normalize_dates(new_data)], ignore_index=True)
    save_price_data(combined, path)
    return True, len(new_data)


def create_new_file(
    path: Path,
    start: date,
    end: date,
    tickers: list[str],
    fetch_prices_fn: Callable[[list[str], date, date], pd.DataFrame],
    stats: DownloadStats,
) -> tuple[bool, int]:
    """Create new monthly file. Return (created, row_count)."""
    data = fetch_prices_fn(tickers, start, end)
    if data.empty:
        return False, 0
    save_price_data(normalize_dates(data), path)
    return True, len(data)


def cleanup_existing_files(data_dir: Path) -> int:
    """Remove all PRICES_*.csv under data_dir. Return count deleted."""
    data_dir = Path(data_dir)
    deleted = 0
    for f in data_dir.rglob("PRICES_*.csv"):
        f.unlink()
        deleted += 1
    return deleted


def determine_start_date(
    data_dir: Path,
    start_date: date,
    force_redownload: bool,
) -> tuple[date, date | None]:
    """
    Start date for this run and last month found (if resuming).
    Returns (start_date, last_month_or_none).
    """
    if force_redownload:
        n = cleanup_existing_files(data_dir)
        if n:
            print(f"Deleted {n} files.")
        return start_date, None
    last = find_last_month_with_data(data_dir)
    return last or start_date, last
