# indicators/atr.py
from typing import Union, Iterable
import numpy as np
import pandas as pd

def _to_series_float(x) -> pd.Series:
    """Normalize input to pandas Series of float, preserving index if possible."""
    if isinstance(x, pd.Series):
        ser = x.astype(float).reset_index(drop=True)
        ser.index = x.index  # keep original index
        return ser.astype(float)
    else:
        return pd.Series(list(x), dtype=float)

def compute_atr_wilder(tr_values: Union[pd.Series, Iterable, np.ndarray],
                       n: int = 14) -> pd.Series:
    """
    Compute ATR using Wilder's smoothing.

    Args:
        tr_values: sequence (Series/array-like) of True Range values (>=0).
        n: period for ATR (default 14).

    Returns:
        pd.Series of ATR values, same index length as input. For indices < n-1 -> NaN.
        The first valid ATR is placed at index (n-1) and equals mean(TR[0:n]).
    """
    if n <= 0:
        raise ValueError("n must be positive integer")

    tr = _to_series_float(tr_values)
    length = len(tr)

    # allocate result as float with NaN
    atr = pd.Series([np.nan] * length, index=tr.index, dtype=float)

    if length < n:
        # not enough data -> all NaN
        return atr

    # compute first ATR value as arithmetic mean of first n TRs
    first_slice = tr.iloc[0:n].astype(float)
    first_atr = float(first_slice.mean())

    # place at index n-1
    atr.iloc[n-1] = first_atr

    # Wilder smoothing recurrence: ATR_t = ( (ATR_{t-1}*(N-1)) + TR_t ) / N
    prev_atr = first_atr
    K = float(n)

    for i in range(n, length):
        tr_t = float(tr.iloc[i])
        curr_atr = ((prev_atr * (K - 1.0)) + tr_t) / K
        atr.iloc[i] = curr_atr
        prev_atr = curr_atr

    return atr
