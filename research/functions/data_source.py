"""
Pluggable data source for price downloads. Replace fetch_prices to use another provider.
Must return a DataFrame with columns: date, ticker, open, high, low, close, volume, adj_close.
"""

from datetime import date
import io
import logging
import sys
from contextlib import contextmanager

import pandas as pd
import yfinance as yf

# Suppress yfinance verbose logging
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

PRICE_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume", "adj_close"]


@contextmanager
def suppress_stderr():
    """Suppress stderr output (e.g., yfinance warnings about non-trading days)."""
    old_stderr = sys.stderr
    try:
        sys.stderr = io.StringIO()
        yield
    finally:
        sys.stderr = old_stderr


def fetch_prices(tickers: list[str], start_date: date, end_date: date) -> pd.DataFrame:
    """
    Fetch OHLCV + adj_close from yfinance.
    
    Args:
        tickers: List of ticker symbols
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
    
    Returns:
        DataFrame with columns: date, ticker, open, high, low, close, volume, adj_close
    """
    if not tickers:
        return _empty_prices_df()

    try:
        with suppress_stderr():
            data = yf.download(
                tickers,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                group_by="ticker",
                auto_adjust=False,
                progress=False,
                threads=True,
            )
    except Exception as e:
        print(f"ERROR: Failed to download data: {e}")
        return _empty_prices_df()
    
    if data.empty:
        return _empty_prices_df()

    # Convert to long format
    df = _convert_to_long_format(data, tickers)
    
    # Ensure all required columns exist
    return _ensure_price_columns(df)


def _empty_prices_df() -> pd.DataFrame:
    """Return an empty DataFrame with the correct price columns."""
    return pd.DataFrame(columns=PRICE_COLUMNS)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to lowercase with underscores."""
    df.columns = [
        c[0].lower().replace(" ", "_") if isinstance(c, tuple) 
        else str(c).lower().replace(" ", "_")
        for c in df.columns
    ]
    return df.rename(columns={"adj close": "adj_close"})


def _convert_to_long_format(data: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Convert yfinance output to long format with one row per ticker-date."""
    if len(tickers) == 1:
        return _process_single_ticker(data, tickers[0])
    return _process_multiple_tickers(data, tickers)


def _process_single_ticker(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Process data for a single ticker."""
    try:
        df = data.reset_index().rename(columns={"Date": "date"})
        df["ticker"] = ticker
        df = _normalize_columns(df)
        
        if df.dropna(how="all").empty:
            return _empty_prices_df()
        
        return df
    except Exception as e:
        print(f"ERROR: Failed to process ticker {ticker}: {e}")
        return _empty_prices_df()


def _process_multiple_tickers(data: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Process data for multiple tickers."""
    dfs = []
    
    for ticker in tickers:
        try:
            # Extract ticker data from multi-index columns
            if isinstance(data.columns, pd.MultiIndex):
                ticker_data = data.xs(ticker, axis=1, level=0)
            else:
                ticker_data = data[ticker]
            
            # Skip if no valid data
            ticker_data = ticker_data.dropna(how="all")
            if ticker_data.empty:
                continue
            
            # Format the data
            df = ticker_data.reset_index()
            df["ticker"] = ticker
            df = _normalize_columns(df)
            
            # Ensure 'date' column exists
            if "date" not in df.columns and len(df.columns) > 0:
                df = df.rename(columns={df.columns[0]: "date"})
            
            dfs.append(df)
            
        except (KeyError, Exception):
            # Silently skip tickers with no data
            continue
    
    if not dfs:
        return _empty_prices_df()
    
    return pd.concat(dfs, ignore_index=True)


def _ensure_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all required columns exist and return only those columns."""
    for col in PRICE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[PRICE_COLUMNS]
