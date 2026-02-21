"""
Train / validation / test data splitting with warm-up overlap.

Uses functions.splits for generic ratio boundaries and warm-up prepending.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import pandas as pd

from functions.splits import prepend_warm_up, train_val_test_end_indices


@dataclass
class SplitResult:
    """Result of splitting a wide DataFrame into train / validation / test."""

    train_df: pd.DataFrame
    val_df: pd.DataFrame
    test_df: pd.DataFrame
    train_range: tuple[pd.Timestamp, pd.Timestamp]
    val_range: tuple[pd.Timestamp, pd.Timestamp]
    test_range: tuple[pd.Timestamp, pd.Timestamp]
    warm_up_bars: int = 0

    def trim_warm_up_val(self) -> pd.DataFrame:
        """Return val_df with the first warm_up_bars rows removed (evaluation range only)."""
        if self.warm_up_bars <= 0 or self.val_df.empty:
            return self.val_df
        return self.val_df.iloc[self.warm_up_bars :]

    def trim_warm_up_test(self) -> pd.DataFrame:
        """Return test_df with the first warm_up_bars rows removed (evaluation range only)."""
        if self.warm_up_bars <= 0 or self.test_df.empty:
            return self.test_df
        return self.test_df.iloc[self.warm_up_bars :]


def split_by_ratio(
    df: pd.DataFrame,
    train_ratio: float,
    val_ratio: float,
    lookback: int = 0,
) -> SplitResult:
    """Split DataFrame by row-count ratios. Optionally add warm-up overlap.

    train_ratio, val_ratio: fractions of rows (e.g. 0.6, 0.2 → test gets 0.2).
    lookback: if > 0, prepend this many bars from the preceding segment to val and test,
    and set warm_up_bars on the result so the engine can trim when computing PnL/metrics.
    """
    if df.empty:
        return SplitResult(
            train_df=pd.DataFrame(),
            val_df=pd.DataFrame(),
            test_df=pd.DataFrame(),
            train_range=(pd.NaT, pd.NaT),
            val_range=(pd.NaT, pd.NaT),
            test_range=(pd.NaT, pd.NaT),
            warm_up_bars=0,
        )
    n = len(df)
    t_end, v_end = train_val_test_end_indices(n, train_ratio, val_ratio)
    train_df = df.iloc[: t_end + 1]
    val_df = df.iloc[t_end + 1 : v_end + 1]
    test_df = df.iloc[v_end + 1 :]

    train_range = (train_df.index.min(), train_df.index.max()) if not train_df.empty else (pd.NaT, pd.NaT)
    val_range = (val_df.index.min(), val_df.index.max()) if not val_df.empty else (pd.NaT, pd.NaT)
    test_range = (test_df.index.min(), test_df.index.max()) if not test_df.empty else (pd.NaT, pd.NaT)

    result = SplitResult(
        train_df=train_df,
        val_df=val_df,
        test_df=test_df,
        train_range=train_range,
        val_range=val_range,
        test_range=test_range,
        warm_up_bars=0,
    )

    if lookback > 0:
        result = _add_warm_up(result, lookback)
    return result


def split_by_dates(
    df: pd.DataFrame,
    train_end: date | pd.Timestamp | str,
    val_end: date | pd.Timestamp | str,
    lookback: int = 0,
) -> SplitResult:
    """Split DataFrame by explicit end dates. Optionally add warm-up overlap.

    train_end: last date (inclusive) of train. val_end: last date (inclusive) of validation.
    Rows after val_end are test. Optionally prepend lookback bars to val and test.
    """
    if df.empty:
        return SplitResult(
            train_df=pd.DataFrame(),
            val_df=pd.DataFrame(),
            test_df=pd.DataFrame(),
            train_range=(pd.NaT, pd.NaT),
            val_range=(pd.NaT, pd.NaT),
            test_range=(pd.NaT, pd.NaT),
            warm_up_bars=0,
        )
    train_end_ts = pd.Timestamp(train_end)
    val_end_ts = pd.Timestamp(val_end)

    train_df = df.loc[df.index <= train_end_ts]
    val_df = df.loc[(df.index > train_end_ts) & (df.index <= val_end_ts)]
    test_df = df.loc[df.index > val_end_ts]

    train_range = (train_df.index.min(), train_df.index.max()) if not train_df.empty else (pd.NaT, pd.NaT)
    val_range = (val_df.index.min(), val_df.index.max()) if not val_df.empty else (pd.NaT, pd.NaT)
    test_range = (test_df.index.min(), test_df.index.max()) if not test_df.empty else (pd.NaT, pd.NaT)

    result = SplitResult(
        train_df=train_df,
        val_df=val_df,
        test_df=test_df,
        train_range=train_range,
        val_range=val_range,
        test_range=test_range,
        warm_up_bars=0,
    )

    if lookback > 0:
        result = _add_warm_up(result, lookback)
    return result


def _add_warm_up(split: SplitResult, lookback: int) -> SplitResult:
    """Prepend lookback bars from the preceding segment to val and test; set warm_up_bars."""
    val_extended = prepend_warm_up(split.val_df, split.train_df, lookback)
    # For test, use val (original segment) for warm-up; if val is empty, use train
    test_preceding = split.val_df if not split.val_df.empty else split.train_df
    test_extended = prepend_warm_up(split.test_df, test_preceding, lookback)
    return SplitResult(
        train_df=split.train_df,
        val_df=val_extended,
        test_df=test_extended,
        train_range=split.train_range,
        val_range=split.val_range,
        test_range=split.test_range,
        warm_up_bars=lookback,
    )
