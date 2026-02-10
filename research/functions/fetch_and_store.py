"""
Shared fetch-and-store logic used by both download_prices and backfill_prices notebooks.

One API call per ticker (full date range), with:
  - Retry + exponential backoff on failure / empty response
  - Adaptive delay: short normally, longer after a rate-limit signal
  - Optional per-ticker date filter (backfill keeps only gap dates)
  - Merge into monthly PRICES CSVs via download_helper
"""

import time
from datetime import date
from pathlib import Path
from typing import Callable

import pandas as pd

from research.functions.data_source import fetch_prices, PRICE_COLUMNS
from research.functions.download_helper import (
    merge_ticker_data_into_monthly_files,
    normalize_dates,
)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_and_store(
    ticker_ranges: dict[str, tuple[date, date]],
    data_dir: Path,
    *,
    filter_dates: dict[str, set[date]] | None = None,
    base_delay: float = 0.3,
    max_retries: int = 3,
    on_ticker: Callable[[str, int], None] | None = None,
) -> dict[str, int]:
    """
    Fetch price data and merge into monthly CSVs.

    Args:
        ticker_ranges: {ticker: (start, end)} — date range to fetch per ticker.
                       `end` follows yfinance convention (exclusive).
        data_dir:      Root data directory (contains year subfolders).
        filter_dates:  Optional {ticker: set of dates} — only keep rows on these
                       dates. Use for backfill to discard non-gap dates.
        base_delay:    Seconds to wait between successful calls (default 0.3).
        max_retries:   Retries per ticker on failure/empty (default 3).
        on_ticker:     Callback(ticker, rows_stored) after each ticker finishes.

    Returns:
        {ticker: rows_stored} for tickers where data was written.
    """
    data_dir = Path(data_dir)
    result: dict[str, int] = {}
    delay = base_delay

    for ticker, (start, end) in ticker_ranges.items():
        df = _fetch_with_retry(ticker, start, end, max_retries, delay)

        if df.empty:
            # Nothing came back even after retries — bump delay for next ticker
            delay = min(delay * 2, 5.0)
            continue

        # Optional: keep only specific dates (backfill use case)
        if filter_dates and ticker in filter_dates:
            df = normalize_dates(df)
            keep = filter_dates[ticker]
            df = df[df["date"].isin(keep)]
            if df.empty:
                continue

        merge_ticker_data_into_monthly_files(data_dir, df)
        rows = len(df)
        result[ticker] = rows

        if on_ticker:
            on_ticker(ticker, rows)

        # Successful call — decay delay back toward base
        delay = max(base_delay, delay * 0.8)
        time.sleep(delay)

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_with_retry(
    ticker: str,
    start: date,
    end: date,
    max_retries: int,
    current_delay: float,
) -> pd.DataFrame:
    """
    Call fetch_prices for a single ticker with retry + exponential backoff.
    Returns the DataFrame (possibly empty if all retries exhausted).
    """
    backoff = current_delay
    for attempt in range(1, max_retries + 1):
        df = fetch_prices([ticker], start, end)
        if not df.empty:
            return df
        # Empty result — could be rate-limited or genuinely no data
        if attempt < max_retries:
            time.sleep(backoff)
            backoff = min(backoff * 2, 10.0)
    return pd.DataFrame(columns=PRICE_COLUMNS)
