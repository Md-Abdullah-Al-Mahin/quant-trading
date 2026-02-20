"""
Shared fetch-and-store logic used by both download_prices and backfill_prices notebooks.

Supports **batch yfinance calls** (multiple tickers in one API call) and
**multi-range per ticker** (e.g. several gap windows for backfill).

Features:
  - Batch tickers that share the same date range into one yfinance call
  - Retry + exponential backoff on failure / empty response
  - Adaptive delay: short normally, longer after a rate-limit signal
  - Optional per-ticker date filter (backfill keeps only gap dates)
  - Merge into monthly PRICES CSVs via download_helper
  - FetchResult reports stored rows and failed tickers
"""

import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Callable, Union

import pandas as pd

from research.functions.data_source import fetch_prices, PRICE_COLUMNS
from research.functions.download_helper import (
    get_month_path,
    load_existing,
    normalize_dates,
    save_price_data,
)

@dataclass
class FetchResult:
    """Outcome of a fetch_and_store run."""
    stored: dict[str, int] = field(default_factory=dict)
    failed: list[str] = field(default_factory=list)


TickerRanges = dict[str, Union[tuple[date, date], list[tuple[date, date]]]]


def _append_to_monthly_accumulator(
    new_data_by_month: dict[tuple[int, int], pd.DataFrame],
    df: pd.DataFrame,
) -> None:
    """Append price rows into new_data_by_month keyed by (year, month)."""
    if df.empty or "date" not in df.columns:
        return
    df = normalize_dates(df.copy())
    df["_year"] = pd.to_datetime(df["date"]).dt.year
    df["_month"] = pd.to_datetime(df["date"]).dt.month
    for (year, month), grp in df.groupby(["_year", "_month"]):
        key = (int(year), int(month))
        add = grp.drop(columns=["_year", "_month"])
        if key not in new_data_by_month:
            new_data_by_month[key] = pd.DataFrame(columns=PRICE_COLUMNS)
        new_data_by_month[key] = pd.concat(
            [new_data_by_month[key], add], ignore_index=True
        )


def fetch_and_store(
    ticker_ranges: TickerRanges,
    data_dir: Path,
    *,
    filter_dates: dict[str, set[date]] | None = None,
    min_date_per_ticker: dict[str, date] | None = None,
    batch_size: int = 20,
    base_delay: float = 0.3,
    max_retries: int = 3,
    on_ticker: Callable[[str, int], None] | None = None,
) -> FetchResult:
    """
    Fetch price data and merge into monthly CSVs.

    Args:
        ticker_ranges: {ticker: (start, end)} or {ticker: [(start, end), ...]}
                       Date ranges to fetch per ticker. ``end`` follows yfinance
                       convention (exclusive). A single tuple is treated as one
                       range; a list of tuples enables multi-range backfill.
        data_dir:      Root data directory (contains year subfolders).
        filter_dates:  Optional {ticker: set of dates} — only keep rows on these
                       dates. Use for backfill to discard non-gap dates.
        min_date_per_ticker: Optional {ticker: date} — only keep rows with
                       date > min_date_per_ticker[ticker]. Used for coalesced
                       single-range runs to avoid storing already-have data.
        batch_size:    Max tickers per yfinance API call (default 20; 20–50 typical).
        base_delay:    Seconds to wait between successful calls (default 0.3).
        max_retries:   Retries per batch on failure/empty (default 3).
        on_ticker:     Callback(ticker, rows_stored) after each ticker finishes.

    Returns:
        FetchResult with stored row counts and list of failed tickers.
    """
    data_dir = Path(data_dir)
    result = FetchResult()
    delay = base_delay

    normalized: dict[str, list[tuple[date, date]]] = {}
    for ticker, ranges in ticker_ranges.items():
        if isinstance(ranges, list):
            normalized[ticker] = ranges
        else:
            normalized[ticker] = [ranges]

    range_groups: dict[tuple[date, date], list[str]] = {}
    for ticker, range_list in normalized.items():
        for r in range_list:
            range_groups.setdefault(r, []).append(ticker)

    ticker_rows: dict[str, int] = {}
    new_data_by_month: dict[tuple[int, int], pd.DataFrame] = {}

    for (start, end), group_tickers in range_groups.items():
        print(f"Fetching data {start} to {end} for {','.join(group_tickers)}")
        for i in range(0, len(group_tickers), batch_size):
            batch = group_tickers[i : i + batch_size]
            df = _fetch_batch_with_retry(batch, start, end, max_retries, delay)

            if df.empty:
                delay = min(delay * 2, 5.0)
                continue

            for ticker in batch:
                ticker_df = df[df["ticker"] == ticker].copy()
                if ticker_df.empty:
                    continue

                # Optional: keep only specific dates (backfill use case)
                if filter_dates and ticker in filter_dates:
                    ticker_df = normalize_dates(ticker_df)
                    keep = filter_dates[ticker]
                    ticker_df = ticker_df[ticker_df["date"].isin(keep)]
                    if ticker_df.empty:
                        continue

                # Optional: only keep rows after min_date (coalesced single-range runs)
                if min_date_per_ticker is not None and ticker in min_date_per_ticker:
                    ticker_df = normalize_dates(ticker_df)
                    ticker_df = ticker_df[
                        ticker_df["date"] > min_date_per_ticker[ticker]
                    ]
                    if ticker_df.empty:
                        continue

                _append_to_monthly_accumulator(new_data_by_month, ticker_df)
                ticker_rows[ticker] = ticker_rows.get(ticker, 0) + len(ticker_df)

            # Successful call — decay delay back toward base
            delay = max(base_delay, delay * 0.8)
            time.sleep(delay)

    # Merge accumulated data into monthly CSVs (one read/write per affected month)
    for (year, month) in sorted(new_data_by_month.keys()):
        path = get_month_path(data_dir, year, month)
        combined = new_data_by_month[(year, month)]
        if path.exists():
            existing = load_existing(path)
            combined = pd.concat([existing, combined], ignore_index=True)
        save_price_data(combined, path)

    all_tickers = set(ticker_ranges.keys())
    for ticker in all_tickers:
        rows = ticker_rows.get(ticker, 0)
        if rows > 0:
            result.stored[ticker] = rows
            if on_ticker:
                on_ticker(ticker, rows)
        else:
            result.failed.append(ticker)

    return result

def _fetch_batch_with_retry(
    tickers: list[str],
    start: date,
    end: date,
    max_retries: int,
    current_delay: float,
) -> pd.DataFrame:
    """
    Call fetch_prices for a batch of tickers with retry + exponential backoff.
    Returns the DataFrame (possibly empty if all retries exhausted).
    """
    backoff = current_delay
    for attempt in range(1, max_retries + 1):
        df = fetch_prices(tickers, start, end)
        if not df.empty:
            return df
        # Empty result — could be rate-limited or genuinely no data
        if attempt < max_retries:
            time.sleep(backoff)
            backoff = min(backoff * 2, 10.0)
    return pd.DataFrame(columns=PRICE_COLUMNS)
