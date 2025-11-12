# analyzer/indicators/ema.py
from typing import List, Optional, Sequence, Tuple, Union
import numpy as np
import pandas as pd

def ema(prices: Sequence[float], period: int) -> List[Optional[float]]:
    """
    Low-level EMA calculator.
    - prices: sequence (list/Series) of floats
    - period: int
    Returns list length == len(prices) with None for indices before seed.
    """
    if period <= 0:
        raise ValueError("period must be > 0")
    n = len(prices)
    ema_vals: List[Optional[float]] = [None] * n
    alpha = 2.0 / (period + 1)
    if n >= period:
        sma = sum(float(p) for p in prices[:period]) / float(period)
        ema_vals[period - 1] = float(sma)
        prev = float(sma)
        for i in range(period, n):
            prev = alpha * float(prices[i]) + (1 - alpha) * prev
            ema_vals[i] = float(prev)
    return ema_vals

def add_ema(df: pd.DataFrame,
            spans: Union[Tuple[int, ...], Sequence[int]] = (9, 21),
            price_col: str = "close",
            prefix: str = "ema",
            force: bool = False) -> pd.DataFrame:
    """
    High-level helper that computes EMA(s) and adds columns to df.
    - df: pandas DataFrame with price_col column
    - spans: tuple/list of integer periods, e.g. (9,21)
    - price_col: column name in df to use as price ('close' by default)
    - prefix: column prefix, result columns will be f"{prefix}_{span}"
    - force: if True overwrite existing columns
    Returns df (modified in-place and returned)
    """
    if price_col not in df.columns:
        raise ValueError(f"price_col '{price_col}' not found in DataFrame")

    # ensure spans is iterable
    if isinstance(spans, int):
        spans = (spans,)
    spans = tuple(int(s) for s in spans)

    prices = df[price_col].tolist()
    for span in spans:
        colname = f"{prefix}_{span}"
        if (colname in df.columns) and not force:
            # skip if exists and not forcing overwrite
            continue
        vals = ema(prices, span)
        # convert None -> np.nan for storing in pandas
        df[colname] = [np.nan if v is None else float(v) for v in vals]
    return df

def ema_cross_buy(ema_fast, ema_slow):
    """
    Detect single-bar bullish EMA cross.
    - ema_fast, ema_slow: sequences (list/Series) of numbers or None
    Returns list[bool] of same length. True at index i if:
      ema_fast[i-1] <= ema_slow[i-1] and ema_fast[i] > ema_slow[i]
    If any required value is None or NaN -> treat as missing and result False.
    """
    from math import isnan

    n = max(len(ema_fast), len(ema_slow)) if (hasattr(ema_fast, '__len__') and hasattr(ema_slow, '__len__')) else 0
    # normalize lengths: assume inputs already same length; if not, use min length
    try:
        n = min(len(ema_fast), len(ema_slow))
    except Exception:
        n = 0

    out = [False] * n
    for i in range(1, n):
        a_prev = ema_fast[i-1]
        a_curr = ema_fast[i]
        b_prev = ema_slow[i-1]
        b_curr = ema_slow[i]

        # treat None or NaN as missing
        missing = False
        for v in (a_prev, a_curr, b_prev, b_curr):
            if v is None:
                missing = True
                break
            # check NaN (works for floats)
            try:
                if isinstance(v, float) and isnan(v):
                    missing = True
                    break
            except Exception:
                pass
        if missing:
            continue

        try:
            if (a_prev <= b_prev) and (a_curr > b_curr):
                out[i] = True
        except Exception:
            # on any comparison error, remain False
            continue
    return out
