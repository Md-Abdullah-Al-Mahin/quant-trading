"""
Research constants. UNIVERSE_DF = all rows from research/raw/*.csv.
Use get_universe(sectors=..., tickers=...) to get a ticker list like UNIVERSE.
"""
from pathlib import Path
import pandas as pd

_RAW_DIR = Path(__file__).resolve().parent.parent / "raw"
_csvs = sorted(_RAW_DIR.glob("*.csv"))
UNIVERSE_DF = pd.concat([pd.read_csv(p) for p in _csvs], ignore_index=True) if _csvs else pd.DataFrame()

def get_universe(sectors=None, tickers=None):
    """Return list of tickers filtered by sector(s) and/or exact ticker(s). Same format as UNIVERSE."""
    df = UNIVERSE_DF
    if sectors is not None:
        s = [sectors] if isinstance(sectors, str) else sectors
        df = df[df["Sector"].isin(s)]
    if tickers is not None:
        t = [tickers] if isinstance(tickers, str) else tickers
        df = df[df["Ticker"].isin(t)]
    return df["Ticker"].drop_duplicates().tolist()

UNIVERSE = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
START_DATE = "2020-01-01"
