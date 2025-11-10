# analyzer/signals.py
"""
Signal helpers and compatibility wrappers.

Includes:
- cross_up, cross_down
- generate_ema_signals (compatibility/simple EMA crossover)
- generate_signals (combined rule with EMA/RSI/MACD/ATR confirmation)
"""
from typing import Mapping, Any
import pandas as pd

# import indicator functions (must exist)
from analyzer.indicators import add_ema, add_rsi, add_macd, add_atr


def cross_up(a: pd.Series, b: pd.Series) -> pd.Series:
    """True where series a crosses above series b on this bar."""
    return (a > b) & (a.shift(1) <= b.shift(1))


def cross_down(a: pd.Series, b: pd.Series) -> pd.Series:
    """True where series a crosses below series b on this bar."""
    return (a < b) & (a.shift(1) >= b.shift(1))


def generate_ema_signals(df: pd.DataFrame, short: int = 9, long: int = 21, price_col: str = "close") -> pd.DataFrame:
    """
    Backwards-compatible helper: compute two EMAs and simple 'buy'/'sell' signal
    based only on EMA crossover.
    """
    if price_col not in df.columns:
        raise ValueError(f"price_col '{price_col}' not found")

    # ensure EMAs exist (compute)
    add_ema(df, spans=(short, long), price_col=price_col)

    df[f"ema_{short}"] = df[f"ema_{short}"].astype(float)
    df[f"ema_{long}"] = df[f"ema_{long}"].astype(float)

    df["signal"] = "none"
    up = cross_up(df[f"ema_{short}"], df[f"ema_{long}"])
    down = cross_down(df[f"ema_{short}"], df[f"ema_{long}"])
    df.loc[up, "signal"] = "buy"
    df.loc[down, "signal"] = "sell"
    return df


def _ensure_indicators(df: pd.DataFrame, cfg: Mapping[str, Any], force: bool = False) -> pd.DataFrame:
    """
    Ensure required indicators exist. Will compute missing ones.
    """
    cfg = dict(cfg or {})
    # EMA spans
    ema_spans = tuple(cfg.get("ema_spans", (9, 21)))
    short = int(cfg.get("short", ema_spans[0] if ema_spans else 9))
    long = int(cfg.get("long", ema_spans[1] if len(ema_spans) > 1 else short * 2))

    # compute EMAs for requested spans
    add_ema(df, spans=tuple(set(ema_spans + (short, long))), price_col='close')

    # RSI
    rsi_period = int(cfg.get("rsi_period", 14))
    if f"rsi_{rsi_period}" not in df.columns or force:
        add_rsi(df, period=rsi_period, price_col='close')

    # MACD
    macd_cfg = dict(cfg.get("macd", {"fast": 12, "slow": 26, "signal": 9}))
    macd_cols = ("macd", "macd_signal", "macd_hist")
    if any(c not in df.columns for c in macd_cols) or force:
        add_macd(df,
                 fast=int(macd_cfg.get("fast", 12)),
                 slow=int(macd_cfg.get("slow", 26)),
                 signal=int(macd_cfg.get("signal", 9)),
                 price_col='close')

    # ATR (optional, ignore errors if OHLC not present)
    atr_period = int(cfg.get("atr_period", 14))
    try:
        if f"atr_{atr_period}" not in df.columns or force:
            add_atr(df, period=atr_period, high_col='high', low_col='low', close_col='close')
    except Exception:
        # ignore if high/low not available
        pass

    return df


def generate_signals(df: pd.DataFrame, config: Mapping[str, Any] = None, force_indicators: bool = False) -> pd.DataFrame:
    """
    Composite signal generator with optional RSI enforcement.
    """
    cfg = dict(config or {})

    short = int(cfg.get("short", 9))
    long = int(cfg.get("long", 21))
    rsi_period = int(cfg.get("rsi_period", 14))
    rsi_overbought = float(cfg.get("rsi_overbought", 70.0))
    rsi_oversold = cfg.get("rsi_oversold", None)
    # new flag: only enforce RSI condition when require_rsi is True
    require_rsi = bool(cfg.get("require_rsi", False))

    macd_confirm = bool(cfg.get("macd_confirm", True))
    atr_min = cfg.get("atr_min", None)
    atr_period = int(cfg.get("atr_period", 14))

    # ensure indicators available (compute if missing)
    df = _ensure_indicators(df, {"ema_spans": (short, long),
                                  "rsi_period": rsi_period,
                                  "macd": cfg.get("macd", {"fast": 12, "slow": 26, "signal": 9}),
                                  "atr_period": atr_period}, force=force_indicators)

    ema_short_col = f"ema_{short}"
    ema_long_col = f"ema_{long}"
    rsi_col = f"rsi_{rsi_period}"
    macd_hist_col = "macd_hist"
    macd_col = "macd"
    macd_signal_col = "macd_signal"
    atr_col = f"atr_{atr_period}"

    df["signal"] = "none"

    # EMA crossover up
    ema_buy = cross_up(df[ema_short_col], df[ema_long_col])

    # RSI check (only if required)
    rsi_ok = pd.Series(True, index=df.index)
    if require_rsi and rsi_col in df.columns:
        rsi_ok = (df[rsi_col] < rsi_overbought)
        if rsi_oversold is not None:
            rsi_ok = rsi_ok & (df[rsi_col] > float(rsi_oversold))

    # MACD confirmation (relaxed: >= 0 accepted)
    macd_ok = pd.Series(True, index=df.index)
    if macd_confirm and macd_hist_col in df.columns and macd_col in df.columns and macd_signal_col in df.columns:
        macd_ok = (df[macd_hist_col] >= 0) | (df[macd_col] > df[macd_signal_col])

    # ATR filter (optional)
    atr_ok = pd.Series(True, index=df.index)
    if atr_min is not None and atr_col in df.columns:
        atr_ok = df[atr_col] > float(atr_min)

    buy_mask = ema_buy & rsi_ok & macd_ok & atr_ok
    df.loc[buy_mask, "signal"] = "buy"
    return df

# compatibility alias: jika modul lain mengimpor add_indicators dari analyzer.signals
try:
    from analyzer.indicators import add_all_indicators as add_indicators
except Exception:
    # fallback: define a no-op shim to avoid ImportError during import time
    def add_indicators(df, config=None, force=False):
        """
        Compatibility shim: delegate to analyzer.indicators.add_all_indicators.
        If analyzer.indicators is not available, return df unchanged.
        """
        # lazy import to give clearer error if module missing
        try:
            from analyzer.indicators import add_all_indicators
        except Exception:
            return df
        return add_all_indicators(df, config=config, force=force)
