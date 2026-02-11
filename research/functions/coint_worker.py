"""
Worker for parallel cointegration tests. Used by ProcessPoolExecutor;
initializer sets _wide so worker processes have the panel without per-task pickling.
"""
import warnings

from statsmodels.tsa.stattools import coint
from statsmodels.tools.sm_exceptions import CollinearityWarning

MIN_OBS = 100
_wide = None


def init_worker(wide_df):
    global _wide
    _wide = wide_df


def test_pair(t1, t2):
    """Run coint for one pair. Returns result dict or None if too few observations."""
    s = _wide[[t1, t2]].dropna()
    if len(s) < MIN_OBS:
        return None
    y0, y1 = s[t1].values, s[t2].values
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always", CollinearityWarning)
        try:
            coint_t, pvalue, crit_values = coint(y0, y1, autolag=None, maxlag=0)
            collinear = any(issubclass(x.category, CollinearityWarning) for x in w)
            return {
                "ticker1": t1,
                "ticker2": t2,
                "coint_t": coint_t,
                "pvalue": pvalue,
                "crit_1pct": float(crit_values[0]) if crit_values is not None else None,
                "crit_5pct": float(crit_values[1]) if crit_values is not None else None,
                "crit_10pct": float(crit_values[2]) if crit_values is not None else None,
                "cointegrated_5pct": pvalue < 0.05,
                "collinear": collinear,
            }
        except Exception as e:
            return {
                "ticker1": t1,
                "ticker2": t2,
                "coint_t": None,
                "pvalue": None,
                "crit_1pct": None,
                "crit_5pct": None,
                "crit_10pct": None,
                "cointegrated_5pct": False,
                "collinear": False,
                "error": str(e),
            }
