# tests/test_backtest_multiple_trades.py
import pandas as pd
import numpy as np
from backtest.simple_backtest import simple_backtest

def sample_multi():
    # Create three ramps with buys before each ramp
    close = []
    high = []
    low = []
    open_ = []
    signals = []
    times = pd.date_range("2025-01-01", periods=60, freq="min")
    for i in range(60):
        # default
        sig = "none"

        # define price behavior
        if 10 <= i < 20:
            c = 100 + (i - 10) * 1.0
        elif 30 <= i < 40:
            c = 110 + (i - 30) * 1.0
        elif 50 <= i < 59:
            c = 120 + (i - 50) * 1.0
        else:
            c = 100.0

        # set buy signals *before* each ramp starts (at indices 9, 29, 49)
        if i in (9, 29, 49):
            sig = "buy"

        close.append(c)
        high.append(c + 0.5)
        low.append(c - 0.5)
        open_.append(close[-2] if len(close) > 1 else close[0])
        signals.append(sig)

    df = pd.DataFrame({"open": open_, "high": high, "low": low, "close": close}, index=times)
    df["atr_14"] = 1.0
    df["signal"] = signals
    res = simple_backtest(df, tp_atr_mult=1.5, sl_atr_mult=1.0, atr_period=14)
    return res

def test_multi_trades_stats():
    res = sample_multi()
    s = res["summary"]
    # Expect three trades (three buy signals)
    assert s["n_trades"] == 3
    # At least one win
    assert s["wins"] >= 1
