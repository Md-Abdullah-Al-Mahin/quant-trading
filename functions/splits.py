"""
Generic time-series split helpers. No dependencies on backtester or research.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def train_val_test_end_indices(
    n: int,
    train_ratio: float,
    val_ratio: float,
) -> tuple[int, int]:
    """Return (train_end_inclusive, val_end_inclusive) for splitting n rows by ratio.

    Segments: train = [0, train_end_inclusive], val = [train_end_inclusive+1, val_end_inclusive],
    test = [val_end_inclusive+1, n-1]. Ratios should be in (0, 1) and sum to <= 1.
    Clamped so each segment has at least one row when n >= 3.
    """
    if n < 1:
        return (-1, -1)
    train_end = max(0, min(int(n * train_ratio) - 1, n - 3))
    val_end = max(train_end + 1, min(int(n * (train_ratio + val_ratio)) - 1, n - 2))
    return (train_end, val_end)


def prepend_warm_up(
    main: pd.DataFrame,
    preceding: pd.DataFrame,
    n_bars: int,
) -> pd.DataFrame:
    """Prepend the last n_bars rows of preceding to main. Index must be sorted.

    If n_bars <= 0 or preceding is empty, returns main unchanged.
    """
    import pandas as pd

    if n_bars <= 0 or preceding is None or preceding.empty:
        return main
    tail = preceding.tail(n_bars)
    if tail.empty:
        return main
    return pd.concat([tail, main], axis=0)
