# indicators/atr.py
from typing import Optional
import pandas as pd

def compute_tr(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    for c in ("high","low","close"):
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")
    df['high'] = pd.to_numeric(df['high'], errors='raise')
    df['low'] = pd.to_numeric(df['low'], errors='raise')
    df['close'] = pd.to_numeric(df['close'], errors='raise')

    df['prev_close'] = df['close'].shift(1)
    cand1 = df['high'] - df['low']
    cand2 = (df['high'] - df['prev_close']).abs()
    cand3 = (df['low'] - df['prev_close']).abs()
    df['tr'] = pd.concat([cand1, cand2, cand3], axis=1).max(axis=1)
    df = df.drop(columns=['prev_close'])
    if (df['tr'] < 0).any():
        raise ValueError("Computed TR has negative values")
    return df

def compute_atr_wilder(tr_series: pd.Series, n: int) -> pd.Series:
    tr = tr_series.astype(float).reset_index(drop=True)
    atr = pd.Series([float('nan')] * len(tr))
    if len(tr) < n:
        return atr
    first_val = tr.iloc[0:n].mean()
    atr.iloc[n-1] = float(first_val)
    for i in range(n, len(tr)):
        prev_atr = atr.iloc[i-1]
        tr_i = tr.iloc[i]
        atr_val = ((prev_atr * (n - 1)) + tr_i) / n
        atr.iloc[i] = float(atr_val)
    atr.index = tr_series.index
    return atr

def compute_tr_and_atr(df: pd.DataFrame, atr_period: int = 14) -> pd.DataFrame:
    df2 = compute_tr(df)
    df2['atr'] = compute_atr_wilder(df2['tr'], atr_period)
    return df2
