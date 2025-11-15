# tests/test_atr_wilder.py
import math
import pandas as pd
import numpy as np

from indicators.atr import compute_atr_wilder

def isclose(a, b, tol=1e-8):
    return abs(a - b) <= tol

def test_atr_wilder_digit_by_digit():
    # Example N=3, TR = [10, 12, 8, 14]
    tr = pd.Series([10.0, 12.0, 8.0, 14.0])
    n = 3

    atr = compute_atr_wilder(tr, n=n)

    # expected:
    # index 0..1 -> NaN
    # index 2 -> mean(10,12,8) = 10.0
    # index 3 -> ((10*(3-1)) + 14) / 3 = (20 + 14)/3 = 34/3
    expected = [math.nan, math.nan, 10.0, 34.0/3.0]

    assert len(atr) == len(expected)
    for i, exp in enumerate(expected):
        got = atr.iloc[i]
        if math.isnan(exp):
            assert pd.isna(got)
        else:
            assert isclose(float(got), float(exp)), f"ATR mismatch at idx {i}: expected {exp}, got {got}"

def test_atr_wilder_all_zeros():
    # TR all zeros -> ATR should be zero (from first valid index onward)
    tr = pd.Series([0.0, 0.0, 0.0, 0.0, 0.0])
    n = 3
    atr = compute_atr_wilder(tr, n=n)
    # first two are NaN
    assert pd.isna(atr.iloc[0])
    assert pd.isna(atr.iloc[1])
    # from index 2 onward should be 0.0
    assert float(atr.iloc[2]) == 0.0
    assert float(atr.iloc[3]) == 0.0
    assert float(atr.iloc[4]) == 0.0

def test_atr_wilder_short_series():
    # shorter than n -> all NaN
    tr = pd.Series([5.0, 6.0])
    atr = compute_atr_wilder(tr, n=3)
    assert atr.isna().all()
