"""
Generic date/datetime helpers. No dependencies on backtester or research.
"""

from __future__ import annotations

from datetime import date


def parse_date(value: date | str | None) -> date | None:
    """Convert a value to a date, or return None.

    Accepts: None, datetime.date, or an ISO-8601 date string (e.g. "2024-01-15").
    """
    if value is None:
        return None
    return date.fromisoformat(value) if isinstance(value, str) else value


def months_in_range(start: date, end: date):
    """Yield (year, month) tuples for every month in [start, end], inclusive."""
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        yield y, m
        m, y = (m + 1, y) if m < 12 else (1, y + 1)
