# analyzer/indicators/rsi.py
from typing import List, Optional

def rsi(prices: List[float], period: int) -> List[Optional[float]]:
    """
    Low-level RSI calculator using Wilder smoothing.
    - prices: list of floats (close prices)
    - period: RSI period (e.g. 14)
    Returns a list len == len(prices) with None for indices before seed.
    """
    if period <= 0:
        raise ValueError("period must be > 0")
    n = len(prices)
    if n == 0:
        return []

    # convert to floats for safety
    prices_f = [float(x) for x in prices]
    deltas = [prices_f[i] - prices_f[i-1] for i in range(1, n)]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]

    out: List[Optional[float]] = [None] * n

    # need at least period deltas (i.e. period+1 prices) to seed
    if len(deltas) < period:
        return out

    # initial average gain/loss = simple mean of first 'period' gains/losses
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # helper to compute rsi from avg_gain/avg_loss, handle zero loss/gain properly
    def compute_rsi(ag: float, al: float) -> float:
        if ag == 0.0 and al == 0.0:
            return 50.0
        if al == 0.0:
            # return very near 100 but not infinite — use 100.0
            return 100.0
        rs = ag / al
        return 100.0 - (100.0 / (1.0 + rs))

    # first computable RSI corresponds to index period (0-based prices)
    out[period] = compute_rsi(avg_gain, avg_loss)

    # Wilder smoothing for subsequent points
    for i in range(period+1, n):
        g = gains[i-1]
        l = losses[i-1]
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
        out[i] = compute_rsi(avg_gain, avg_loss)

    return out

def rsi_buy_condition(rsi_vals):
    """
    rsi_vals: sequence (list/Series) of floats or None/NaN
    Return list[bool] same length: True if rsi_current > 30 and rsi_current > rsi_prev
    If either current or previous is None/NaN -> False
    """
    import math
    n = len(rsi_vals) if hasattr(rsi_vals, '__len__') else 0
    out = [False] * n
    for i in range(1, n):
        cur = rsi_vals[i]
        prev = rsi_vals[i-1]
        # reject None or NaN
        if cur is None or prev is None:
            continue
        try:
            if isinstance(cur, float) and math.isnan(cur):
                continue
            if isinstance(prev, float) and math.isnan(prev):
                continue
        except Exception:
            pass
        try:
            if float(cur) > 30.0 and float(cur) > float(prev):
                out[i] = True
        except Exception:
            # on any conversion/comparison error, leave False
            continue
    return out
