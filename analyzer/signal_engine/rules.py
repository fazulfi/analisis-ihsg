# analyzer/signal_engine/rules.py
from typing import Any, Dict, List, Mapping, Optional
import pandas as pd

from analyzer.indicators import add_all_indicators, ema_cross_buy, rsi_buy_condition

def generate_signals(df: pd.DataFrame,
                     cfg: Optional[Mapping[str, Any]] = None,
                     price_col: str = "close",
                     ts_col: Optional[str] = None,
                     force_indicators: bool = True,
                     emit_next_open: bool = False) -> List[Dict[str, Any]]:
    """
    Generate BUY-only signals from dataframe using EMA-cross + RSI condition.
    - df: pd.DataFrame with price_col and optional ts_col (or use index).
    - cfg: config mapping, expected keys:
        * 'ema_spans': tuple/list (short, long) or (9,21) default
        * 'short'/'long' optional overrides
        * 'rsi_period': int (default 14)
    - force_indicators: if True, recompute indicators even if present.
    - emit_next_open: if True, signal on bar i will be emitted with ts/price of bar i+1 (skip if i+1 out of range)
    Returns list of dicts: [{'ts':..., 'signal':'BUY', 'price':..., 'index':i}, ...]
    """
    cfg = dict(cfg or {})

    # ensure indicators exist (will compute missing ones)
    # Copy cfg and disable ATR calculation if price frame lacks high/low columns (tests often use 'close' only)
    cfg2 = dict(cfg or {})
    if ('high' not in df.columns) or ('low' not in df.columns):
        # prevent add_all_indicators from trying to compute ATR when data doesn't include high/low
        cfg2['atr_period'] = 0
    add_all_indicators(df, cfg2, force=force_indicators)

    # determine ema spans and rsi period
    ema_spans = tuple(cfg.get("ema_spans", (9, 21)))
    if "short" in cfg:
        short = int(cfg.get("short"))
    else:
        short = int(ema_spans[0]) if len(ema_spans) >= 1 else 9
    if "long" in cfg:
        long = int(cfg.get("long"))
    else:
        long = int(ema_spans[1]) if len(ema_spans) > 1 else max(short * 2, short + 1)

    rsi_period = int(cfg.get("rsi_period", cfg.get("rsi", {}).get("period", 14)))

    col_fast = f"ema_{short}"
    col_slow = f"ema_{long}"
    col_rsi = f"rsi_{rsi_period}"

    # validate columns exist
    if col_fast not in df.columns or col_slow not in df.columns or col_rsi not in df.columns:
        raise ValueError(f"required indicator columns missing: {col_fast},{col_slow},{col_rsi}")

    # For detection, compute EMAs using pandas ewm so we have numeric values early (avoid NaN warmup)
    # but keep df columns (add_all_indicators) unchanged — this makes crossing detection robust on short test series.
    prices_series = pd.Series(df[price_col].astype(float))
    ef = prices_series.ewm(span=short, adjust=False, min_periods=1).mean().tolist()
    es = prices_series.ewm(span=long, adjust=False, min_periods=1).mean().tolist()
    # use stored RSI column (may contain np.nan where not enough data)
    rsi_vals = df[col_rsi].tolist()

    # get boolean series
    crosses = ema_cross_buy(ef, es)
    rsi_cond = rsi_buy_condition(rsi_vals)

    signals: List[Dict[str, Any]] = []
    n = min(len(crosses), len(rsi_cond), len(df))

    for i in range(n):
        # If we have a cross at i, allow RSI confirmation at i or i+1
        if not crosses[i]:
            continue
        # find confirmation index: prefer i, fallback to i+1
        confirm_idx = None
        if i < n and rsi_cond[i]:
            confirm_idx = i
        elif (i + 1) < n and rsi_cond[i+1]:
            confirm_idx = i + 1
        if confirm_idx is None:
            continue
        # apply emit_next_open shift on confirmed index
        target_idx = confirm_idx + 1 if emit_next_open else confirm_idx
        if target_idx >= n:
            continue
        ts = df.iloc[target_idx][ts_col] if ts_col and ts_col in df.columns else df.index[target_idx]
        price = float(df.iloc[target_idx][price_col])
        signals.append({"index": target_idx, "ts": ts, "signal": "BUY", "price": price})
    return signals

