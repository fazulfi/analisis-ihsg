import pandas as pd
import math
import pytest

# Try to import compute_tr (some repos may provide compute_tr or compute_tr_and_atr)
from indicators.atr import compute_tr

def _to_series_tr(maybe):
    # normalize return value to pandas Series of floats
    if isinstance(maybe, pd.DataFrame):
        # try common column names
        for c in ("tr","TR","Tr"):
            if c in maybe.columns:
                return maybe[c].astype(float).reset_index(drop=True)
        # otherwise pick first numeric column
        for c in maybe.columns:
            try:
                return pd.to_numeric(maybe[c], errors='coerce').astype(float).reset_index(drop=True)
            except Exception:
                continue
        raise RuntimeError("compute_tr returned DataFrame without numeric column")
    elif isinstance(maybe, pd.Series):
        return maybe.astype(float).reset_index(drop=True)
    else:
        # maybe numpy array/list
        return pd.Series(maybe, dtype=float)

def test_tr_digit_by_digit():
    # build 4 bars such that high-low create desired TR candidates
    rows = [
        {"date":"2025-01-01","open":1000,"high":1050,"low":1000,"close":1040,"volume":1000},
        {"date":"2025-01-02","open":1040,"high":1100,"low":1040,"close":1075,"volume":1000},
        {"date":"2025-01-03","open":1075,"high":1115,"low":1075,"close":1100,"volume":1000},
        {"date":"2025-01-04","open":1100,"high":1170,"low":1100,"close":1160,"volume":1000},
    ]
    df = pd.DataFrame(rows)
    tr_raw = compute_tr(df)
    tr_series = _to_series_tr(tr_raw)
    expected = [50.0, 60.0, 40.0, 70.0]
    assert len(tr_series) == len(expected)
    for i, e in enumerate(expected):
        assert math.isclose(float(tr_series.iloc[i]), e, rel_tol=1e-9), f"TR mismatch at {i}: got {tr_series.iloc[i]} expected {e}"
