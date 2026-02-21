"""
Generic I/O helpers. No dependencies on backtester or research.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def read_csv_or_none(
    path: Path,
    parse_dates: list[str] | None = None,
) -> pd.DataFrame | None:
    """Read a single CSV into a DataFrame, or return None on any error.

    path : Path to the CSV file.
    parse_dates : Column name(s) to parse as datetime; None = no date parsing.
    """
    import pandas as pd

    try:
        if not path.is_file():
            return None
        kwargs: dict = {}
        if parse_dates is not None:
            kwargs["parse_dates"] = parse_dates
        return pd.read_csv(path, **kwargs)
    except Exception:
        return None
