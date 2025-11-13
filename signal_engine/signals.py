"""
signal_engine/signals.py (configurable)

generate_signals(df, params=None, cfg=None) -> list[dict]

Default logic:
 - EMA crossover (short over long) + RSI confirmation
 - EMA short = 12, EMA long = 26, RSI period = 14
 - BUY when EMA_short crosses above EMA_long AND RSI <= rsi_buy_thresh
 - SELL when EMA_short crosses below EMA_long AND RSI >= rsi_sell_thresh
 - min_signal_distance prevents signals too close
 - returns list of dicts {'index': idx, 'signal_type': 'BUY'|'SELL', 'date': 'YYYY-MM-DD'}

Notes:
 - This generator is intentionally conservative (needs cross + RSI).
 - Postprocessing (single-open enforcement) should be applied afterwards for real trading.
"""
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

DEFAULTS = {
    "ema_short": 12,
    "ema_long": 26,
    "rsi_period": 14,
    "rsi_buy_thresh": 30.0,
    "rsi_sell_thresh": 70.0,
    "min_signal_distance": 5,
    "max_signals": None,
    "only_one_open": False  # recommend using postprocess instead
}

def _ensure_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    # ensure date exists
    if "date" not in df.columns:
        for alt in ("timestamp","time","datetime","date_time"):
            if alt in df.columns:
                df["date"] = pd.to_datetime(df[alt], errors="coerce").dt.strftime("%Y-%m-%d")
                break
    # ensure close exists
    if "close" not in df.columns:
        raise ValueError("generate_signals: dataframe must contain 'close' column")
    # coerce numeric
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    # Wilder-type RSI using ewm smoothing (equivalent)
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = -1 * delta.clip(upper=0.0)
    # smoothing
    gain = up.ewm(alpha=1/period, adjust=False).mean()
    loss = down.ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0)

def _merge_cfg(params: Optional[dict], cfg: Optional[dict]) -> dict:
    out = DEFAULTS.copy()
    if cfg:
        # allow config keys to be nested under "signal_engine" optionally
        se = cfg.get("signal_engine", {}) if isinstance(cfg, dict) else {}
        out.update({k: se.get(k, out[k]) for k in out.keys() if k in se or k in out})
        # fallback: direct keys in cfg
        for k in out.keys():
            if k in cfg:
                out[k] = cfg[k]
    if params:
        out.update(params)
    # coerce numeric types
    out["ema_short"] = int(out["ema_short"])
    out["ema_long"] = int(out["ema_long"])
    out["rsi_period"] = int(out["rsi_period"])
    out["rsi_buy_thresh"] = float(out["rsi_buy_thresh"])
    out["rsi_sell_thresh"] = float(out["rsi_sell_thresh"])
    out["min_signal_distance"] = int(out["min_signal_distance"])
    if out.get("max_signals") is not None:
        out["max_signals"] = int(out["max_signals"])
    return out

def generate_signals(df: pd.DataFrame, params: Optional[dict] = None, cfg: Optional[dict] = None) -> List[Dict]:
    """
    Generate signals from OHLC df.
    Returns list of dicts: {'index': int, 'signal_type': 'BUY'|'SELL', 'date': 'YYYY-MM-DD'}
    """
    settings = _merge_cfg(params, cfg)
    df = _ensure_df(df)

    n = len(df)
    if n < max(settings["ema_short"], settings["ema_long"], settings["rsi_period"]) + 1:
        # not enough data
        return []

    close = df["close"]

    ema_s = _ema(close, settings["ema_short"])
    ema_l = _ema(close, settings["ema_long"])
    rsi = _rsi(close, settings["rsi_period"])

    diff = (ema_s - ema_l).fillna(0.0)
    sign = np.sign(diff)
    prev_sign = sign.shift(1).fillna(0.0)

    cross_up = (prev_sign < 0) & (sign > 0)
    cross_down = (prev_sign > 0) & (sign < 0)

    signals: List[Dict] = []
    last_idx = -99999

    for idx in range(n):
        if settings["max_signals"] is not None and len(signals) >= settings["max_signals"]:
            break
        if idx - last_idx < settings["min_signal_distance"]:
            continue

        if cross_up.iloc[idx]:
            r = float(rsi.iloc[idx])
            if r <= settings["rsi_buy_thresh"]:
                date = str(df.iloc[idx]["date"]) if "date" in df.columns else ""
                signals.append({"index": int(idx), "signal_type": "BUY", "date": date})
                last_idx = idx
        elif cross_down.iloc[idx]:
            r = float(rsi.iloc[idx])
            if r >= settings["rsi_sell_thresh"]:
                date = str(df.iloc[idx]["date"]) if "date" in df.columns else ""
                signals.append({"index": int(idx), "signal_type": "SELL", "date": date})
                last_idx = idx

    return signals

# backward-compatible name
__all__ = ["generate_signals"]
