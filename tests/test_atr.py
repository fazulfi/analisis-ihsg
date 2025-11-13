import pandas as pd
import math
import pytest

from indicators.atr import compute_tr_and_atr, compute_tr

def _get_tr_series(df):
    maybe = compute_tr(df)
    if isinstance(maybe, pd.DataFrame):
        for c in ("tr","TR","Tr"):
            if c in maybe.columns:
                return maybe[c].astype(float).reset_index(drop=True)
        # fallback: first column
        return pd.to_numeric(maybe.iloc[:,0], errors='coerce').astype(float).reset_index(drop=True)
    elif hasattr(maybe, "astype"):
        return maybe.astype(float).reset_index(drop=True)
    else:
        return pd.Series(maybe, dtype=float)

def test_atr_wilder_small_N():
    rows = [
        {"date":"2025-01-01","open":1000,"high":1010,"low":1000,"close":1005,"volume":100},
        {"date":"2025-01-02","open":1005,"high":1017,"low":1005,"close":1010,"volume":100},
        {"date":"2025-01-03","open":1010,"high":1018,"low":1008,"close":1012,"volume":100},
        {"date":"2025-01-04","open":1012,"high":1026,"low":1012,"close":1020,"volume":100},
    ]
    df = pd.DataFrame(rows)
    out = compute_tr_and_atr(df, atr_period=3)
    # get TR series (use the compute_tr helper to be consistent)
    tr_series = _get_tr_series(df)
    # manual Wilder ATR calculation using TR values from compute_tr
    # ATR_first (aligned at index atr_period-1)
    n = 3
    atr_first_expected = float(tr_series.iloc[0:n].mean())
    # next ATR
    atr_next_expected = ((atr_first_expected * (n-1)) + float(tr_series.iloc[n])) / n
    # locate atr in returned df: try column 'atr'
    if 'atr' in out.columns:
        atr_at_2 = float(out.loc[2,'atr'])
        atr_at_3 = float(out.loc[3,'atr'])
    else:
        # maybe returned as numeric column name
        atr_at_2 = float(out.iloc[2].to_dict().get('atr') or out.iloc[2].dropna().iloc[-1])
        atr_at_3 = float(out.iloc[3].to_dict().get('atr') or out.iloc[3].dropna().iloc[-1])
    assert math.isclose(atr_at_2, atr_first_expected, rel_tol=1e-9)
    assert math.isclose(atr_at_3, atr_next_expected, rel_tol=1e-9)
