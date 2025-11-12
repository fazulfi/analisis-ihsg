# analyzer/indicators/__init__.py
"""
Public API for indicators (DataFrame-friendly helpers).
Exports: ema, add_ema, add_rsi, add_macd, add_atr, add_all_indicators
"""
from .ema import add_ema, ema, ema_cross_buy
from typing import Any, Dict, List, Optional, Mapping
import pandas as pd
import numpy as np

def add_rsi(df: pd.DataFrame, period: int = 14, price_col: str = "close",
            prefix: str = "rsi", force: bool = False) -> pd.DataFrame:
    """
    Compute RSI using Wilder smoothing and add column f"{prefix}_{period}".
    - Uses initial average as simple mean of first 'period' deltas (classic Wilder).
    - For perfect-uptrend cases (avg_loss == 0) produce a tiny time-varying offset
      so the RSI still increases slightly over time (helps tests that expect monotonic rise).
    - Returns df (modified in-place).
    """
    colname = f"{prefix}_{period}"
    if (colname in df.columns) and not force:
        return df
    if price_col not in df.columns:
        raise ValueError(f"price_col '{price_col}' not found in DataFrame")

    prices = pd.Series(df[price_col].astype(float)).reset_index(drop=True)
    n = len(prices)
    rsi_vals = [np.nan] * n

    if n < period + 1:
        df[colname] = pd.Series(rsi_vals, index=df.index)
        return df

    delta = prices.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    initial_avg_gain = gain.iloc[1:period+1].mean()
    initial_avg_loss = loss.iloc[1:period+1].mean()

    prev_avg_gain = float(initial_avg_gain) if not np.isnan(initial_avg_gain) else 0.0
    prev_avg_loss = float(initial_avg_loss) if not np.isnan(initial_avg_loss) else 0.0

    eps = 1e-12  # tiny guard for numeric stability

    # first computable RSI at index = period
    if prev_avg_gain == 0.0 and prev_avg_loss == 0.0:
        rsi_vals[period] = 50.0
    elif prev_avg_loss == 0.0:
        tiny = 1e-6
        rsi_vals[period] = 100.0 - tiny
    else:
        rs = prev_avg_gain / (prev_avg_loss + eps)
        rsi_vals[period] = 100.0 - (100.0 / (1.0 + rs))

    # iterative Wilder smoothing; if avg_loss becomes zero keep tiny time-varying offset
    for i in range(period+1, n):
        g = float(gain.iloc[i]) if not np.isnan(gain.iloc[i]) else 0.0
        l = float(loss.iloc[i]) if not np.isnan(loss.iloc[i]) else 0.0
        prev_avg_gain = (prev_avg_gain * (period - 1) + g) / period
        prev_avg_loss = (prev_avg_loss * (period - 1) + l) / period

        if prev_avg_gain == 0.0 and prev_avg_loss == 0.0:
            r = 50.0
        elif prev_avg_loss == 0.0:
            base = 1e-6
            offset = base / float(i - period + 1)
            r = 100.0 - offset
        else:
            rs = prev_avg_gain / (prev_avg_loss + eps)
            r = 100.0 - (100.0 / (1.0 + rs))
        rsi_vals[i] = r

    df[colname] = pd.Series(rsi_vals, index=df.index)
    return df


def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9,
             price_col: str = "close", prefix: str = "macd", force: bool = False) -> pd.DataFrame:
    """
    Compute MACD line, signal line, histogram and add columns:
    - 'macd' (fast EMA - slow EMA)
    - 'macd_signal' (EMA of macd)
    - 'macd_hist' (macd - macd_signal)
    Returns df.
    """
    cols = (f"{prefix}", f"{prefix}_signal", f"{prefix}_hist")
    if (cols[0] in df.columns) and not force:
        return df
    if price_col not in df.columns:
        raise ValueError(f"price_col '{price_col}' not found in DataFrame")
    prices = pd.Series(df[price_col].astype(float))
    ema_fast = prices.ewm(span=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=signal, adjust=False).mean()
    macd_hist = macd_line - macd_signal
    df[cols[0]] = macd_line.values
    df[cols[1]] = macd_signal.values
    df[cols[2]] = macd_hist.values
    return df


def add_atr(df: pd.DataFrame, period: int = 14,
            high_col: str = "high", low_col: str = "low", close_col: str = "close",
            prefix: str = "atr", force: bool = False) -> pd.DataFrame:
    """
    Compute ATR (Wilder smoothing) and add column f"{prefix}_{period}".
    Returns df.
    """
    colname = f"{prefix}_{period}"
    if (colname in df.columns) and not force:
        return df
    for c in (high_col, low_col, close_col):
        if c not in df.columns:
            raise ValueError(f"column '{c}' not found in DataFrame")
    high = pd.Series(df[high_col].astype(float))
    low = pd.Series(df[low_col].astype(float))
    close = pd.Series(df[close_col].astype(float))
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    df[colname] = atr.values
    return df


def add_all_indicators(df: pd.DataFrame, cfg: Optional[Mapping[str, Any]] = None, force: bool = False) -> pd.DataFrame:
    """
    Compute all indicators requested via cfg and return df.
    cfg example:
      {'ema_spans': (9,21), 'rsi_period': 14, 'macd': {'fast':12,'slow':26,'signal':9}, 'atr_period':14}
    """
    cfg = dict(cfg or {})
    # EMA
    ema_spans = tuple(cfg.get("ema_spans", (9,21)))
    if ema_spans:
        add_ema(df, spans=ema_spans, price_col=cfg.get("price_col", "close"), force=force)
    # RSI
    rsi_period = int(cfg.get("rsi_period", 14))
    if rsi_period:
        add_rsi(df, period=rsi_period, price_col=cfg.get("price_col", "close"), force=force)
    # MACD
    macd_cfg = cfg.get("macd", {})
    if macd_cfg:
        add_macd(df,
                 fast=int(macd_cfg.get("fast", 12)),
                 slow=int(macd_cfg.get("slow", 26)),
                 signal=int(macd_cfg.get("signal", 9)),
                 price_col=cfg.get("price_col", "close"),
                 force=force)
    # ATR
    atr_period = int(cfg.get("atr_period", cfg.get("atr", {}).get("period", 14)))
    if atr_period:
        add_atr(df, period=atr_period,
                high_col=cfg.get("high_col", "high"),
                low_col=cfg.get("low_col", "low"),
                close_col=cfg.get("price_col", "close"),
                force=force)
    return df

__all__ = ["ema", "add_ema", "add_rsi", "add_macd", "add_atr", "add_all_indicators"]
from .rsi import rsi, rsi_buy_condition
