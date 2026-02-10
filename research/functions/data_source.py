"""
Pluggable data source for price downloads. Replace fetch_prices to use another provider.
Must return a DataFrame with columns: date, ticker, open, high, low, close, volume, adj_close.
"""

from datetime import date
import pandas as pd
import yfinance as yf

PRICE_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume", "adj_close"]


def fetch_prices(tickers: list[str], start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch OHLCV + adj_close. Override or replace this module to use another source."""
    if not tickers:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    try:
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
        print(f"ERROR: Failed to download data for tickers {tickers}: {e}")
        return pd.DataFrame(columns=PRICE_COLUMNS)
    
    if data.empty:
        print(f"WARNING: No data returned for tickers: {tickers}")
        return pd.DataFrame(columns=PRICE_COLUMNS)

    if len(tickers) == 1:
        out = _single_ticker_to_long(data, tickers[0])
    else:
        out = _multi_ticker_to_long(data, tickers)

    for c in PRICE_COLUMNS:
        if c not in out.columns:
            out[c] = None
    return out[[c for c in PRICE_COLUMNS if c in out.columns]]


def _single_ticker_to_long(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    try:
        out = data.reset_index()
        out["ticker"] = ticker
        out = out.rename(columns={"Date": "date"})
        out.columns = [str(c).lower().replace(" ", "_") for c in out.columns]
        
        if out.dropna(how="all").empty:
            print(f"WARNING: No valid data for ticker: {ticker}")
            return pd.DataFrame(columns=PRICE_COLUMNS)
        
        return out
    except Exception as e:
        print(f"ERROR: Error processing data for ticker {ticker}: {e}")
        return pd.DataFrame(columns=PRICE_COLUMNS)


def _multi_ticker_to_long(data: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    rows = []
    for t in tickers:
        try:
            sub = data[t].copy()
        except (KeyError, TypeError):
            if isinstance(data.columns, pd.MultiIndex):
                try:
                    sub = data.xs(t, axis=1, level=0).copy()
                except KeyError:
                    print(f"WARNING: No data available for ticker: {t}")
                    continue
            else:
                print(f"WARNING: Ticker not found in downloaded data: {t}")
                continue
        except Exception as e:
            print(f"ERROR: Error extracting data for ticker {t}: {e}")
            continue
        
        sub = sub.dropna(how="all")
        if sub.empty:
            print(f"INFO: No valid data rows for ticker: {t}")
            continue
        
        try:
            sub = sub.reset_index()
            sub["ticker"] = t
            sub.columns = [
                c[0].lower().replace(" ", "_") if isinstance(c, tuple) else str(c).lower().replace(" ", "_")
                for c in sub.columns
            ]
            sub = sub.rename(columns={sub.columns[0]: "date"} if "date" not in sub.columns else {})
            rows.append(sub)
        except Exception as e:
            print(f"ERROR: Error formatting data for ticker {t}: {e}")
            continue
    
    if not rows:
        print("WARNING: No valid data for any tickers")
        return pd.DataFrame(columns=PRICE_COLUMNS)
    
    return pd.concat(rows, ignore_index=True).rename(columns={"adj close": "adj_close"})
