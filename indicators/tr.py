# indicators/tr.py
"""
Compute True Range (TR) for OHLC series.

TR = max(
    High - Low,
    abs(High - PrevClose),
    abs(Low - PrevClose)
)

First bar: use High - Low (per spec).
Returns a DataFrame copy with new column `tr_col`.
"""

from typing import Optional
import pandas as pd
import numpy as np


def compute_tr(
    df: pd.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    tr_col: str = "tr",
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Compute True Range and attach as column `tr_col`.

    Parameters
    ----------
    df : pd.DataFrame
        Input OHLC dataframe (must contain high_col, low_col).
        close_col recommended but if missing prev-close based components will be NaN.
    high_col, low_col, close_col : str
        Column names for high/low/close.
    tr_col : str
        Name for the resulting TR column.
    inplace : bool
        If True, write into the input df (and return it). Otherwise returns a copy.

    Returns
    -------
    pd.DataFrame
        DataFrame with TR column added.
    """
    if not inplace:
        df = df.copy()

    if high_col not in df.columns or low_col not in df.columns:
        raise KeyError(f"Missing required column(s): {high_col} and/or {low_col}")

    # ensure numeric
    df[high_col] = pd.to_numeric(df[high_col], errors="coerce")
    df[low_col] = pd.to_numeric(df[low_col], errors="coerce")

    prev_close = None
    if close_col in df.columns:
        df[close_col] = pd.to_numeric(df[close_col], errors="coerce")
        prev_close = df[close_col].shift(1)

    # candidate 1: high - low
    tr1 = (df[high_col] - df[low_col]).abs()

    # candidate 2 & 3 need prev_close; if prev_close missing they will be NaN
    if prev_close is None:
        tr2 = pd.Series([np.nan] * len(df), index=df.index)
        tr3 = pd.Series([np.nan] * len(df), index=df.index)
    else:
        tr2 = (df[high_col] - prev_close).abs()
        tr3 = (df[low_col] - prev_close).abs()

    # elementwise max
    tr_df = pd.concat([tr1, tr2, tr3], axis=1)
    tr_series = tr_df.max(axis=1)

    # For first bar (and any bar where prev_close is NaN) we want to ensure at least high-low:
    # max(...) handles it, but ensure first bar uses tr1 explicitly per spec
    if len(df) > 0:
        tr_series.iloc[0] = tr1.iloc[0]

    # Any negative or weird values -> make absolute and fill NaN with tr1
    tr_series = tr_series.fillna(tr1).abs()

    # ensure numeric float dtype
    df[tr_col] = tr_series.astype(float)

    # final check: all non-negative
    if (df[tr_col] < 0).any():
        raise ValueError("Computed TR has negative values (shouldn't happen)")

    return df
