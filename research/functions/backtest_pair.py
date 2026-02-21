"""
Z-score mean-reversion backtest for one pair. Used by pairs_backtest.ipynb and run_backtest_rank.py.
"""

import numpy as np
import pandas as pd


def backtest_pair(
    wide: pd.DataFrame,
    ticker_a: str,
    ticker_b: str,
    lookback: int,
    entry_z: float,
    exit_z: float,
    capital: float,
    cost_bps: float = 5,
) -> dict:
    """
    Run a simple z-score mean-reversion backtest on one pair.
    Returns dict with summary stats and a daily-PnL Series.

    cost_bps: round-trip transaction cost in basis points, charged on
              the full notional (capital) every time the position changes.
    """
    if ticker_a not in wide.columns or ticker_b not in wide.columns:
        return {"pair": f"{ticker_a}/{ticker_b}", "ticker_a": ticker_a, "ticker_b": ticker_b, "error": "missing ticker"}

    df = wide[[ticker_a, ticker_b]].dropna()
    if len(df) < lookback + 20:
        return {"pair": f"{ticker_a}/{ticker_b}", "ticker_a": ticker_a, "ticker_b": ticker_b, "error": "not enough data"}

    pa, pb = df[ticker_a].values, df[ticker_b].values
    n = len(pa)

    # Rolling OLS hedge ratio β = Cov(x,y)/Var(x) — vectorized (no per-day Python loop)
    x = pd.Series(pb, index=df.index)
    y = pd.Series(pa, index=df.index)
    mean_x = x.rolling(lookback).mean()
    mean_y = y.rolling(lookback).mean()
    mean_xy = (x * y).rolling(lookback).mean()
    var_x = (x * x).rolling(lookback).mean() - mean_x * mean_x
    cov_xy = mean_xy - mean_x * mean_y
    beta_series = cov_xy / var_x.replace(0, np.nan)  # avoid div by zero
    beta_series = beta_series.replace([np.inf, -np.inf], np.nan).ffill()
    beta = beta_series.values
    beta = np.nan_to_num(beta, nan=0.0, posinf=0.0, neginf=0.0)

    spread = pa - beta * pb

    # Rolling z-score of the spread
    s_series = pd.Series(spread, index=df.index)
    s_mean = s_series.rolling(lookback).mean()
    s_std = s_series.rolling(lookback).std()
    z = ((s_series - s_mean) / s_std).values

    # Generate positions: +1 long spread, -1 short spread, 0 flat
    pos = np.zeros(len(z))
    for i in range(1, len(z)):
        if np.isnan(z[i]):
            continue
        prev = pos[i - 1]
        if prev == 0:
            if z[i] < -entry_z:
                pos[i] = 1
            elif z[i] > entry_z:
                pos[i] = -1
        elif prev == 1:
            pos[i] = 0 if z[i] >= -exit_z else 1
        elif prev == -1:
            pos[i] = 0 if z[i] <= exit_z else -1

    # Daily PnL
    spread_chg = np.diff(spread)
    avg_spread = np.nanmean(np.abs(spread[lookback:]))
    scale = capital / avg_spread if avg_spread > 0 else 1.0
    daily_pnl = pos[:-1] * spread_chg * scale

    pos_delta = np.abs(np.diff(pos))
    cost_per_unit = capital * (cost_bps / 10_000)
    daily_cost = pos_delta * cost_per_unit
    daily_pnl -= daily_cost

    pnl_series = pd.Series(daily_pnl, index=df.index[:-1], name=f"{ticker_a}/{ticker_b}")
    pnl_series = pnl_series.iloc[lookback:]

    total_pnl = pnl_series.sum()
    total_cost = float(daily_cost[lookback:].sum())
    mean_daily = pnl_series.mean()
    std_daily = pnl_series.std()
    sharpe = (mean_daily / std_daily * np.sqrt(252)) if std_daily and std_daily > 0 else 0.0
    n_trades = int(np.sum(np.diff(pos[lookback:]) != 0))
    days = len(pnl_series)
    # CAGR: (1 + total_pnl/capital)^(252/days) - 1
    gross = 1.0 + float(total_pnl) / capital
    if gross > 0 and days > 0:
        yearly_growth = (gross ** (252.0 / days)) - 1.0
    else:
        yearly_growth = float("nan")
    yearly_growth_pct = round(yearly_growth * 100, 2)

    return {
        "pair": f"{ticker_a}/{ticker_b}",
        "ticker_a": ticker_a,
        "ticker_b": ticker_b,
        "days": days,
        "total_pnl": round(float(total_pnl), 2),
        "total_cost": round(total_cost, 2),
        "sharpe": round(float(sharpe), 3),
        "yearly_growth_pct": yearly_growth_pct,
        "mean_daily_pnl": round(float(mean_daily), 2),
        "std_daily_pnl": round(float(std_daily), 2),
        "n_trades": n_trades,
        "pnl_series": pnl_series,
    }
