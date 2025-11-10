# tests/test_backtest.py
import pandas as pd
import numpy as np
from backtest.simple_backtest import simple_backtest

def sample_for_backtest():
    # Build series: flat then up spike to trigger tp
    close = [100.0]*5 + list(np.linspace(100, 110, 10)) + [110.0]*5
    high = [c + 0.5 for c in close]
    low = [c - 0.5 for c in close]
    open_ = [close[0]] + close[:-1]
    times = pd.date_range("2025-01-01", periods=len(close), freq="min")
    df = pd.DataFrame({"open": open_, "high": high, "low": low, "close": close}, index=times)
    # add an ATR value (constant for simplicity)
    df["atr_14"] = 1.0
    # set a buy signal on a bar before ramp up starts
    df["signal"] = "none"
    df.at[times[4], "signal"] = "buy"  # buy at next open (times[5] open=100)
    return df

def test_simple_backtest_single_trade_tp_hit():
    df = sample_for_backtest()
    res = simple_backtest(df, tp_atr_mult=2.0, sl_atr_mult=1.0, atr_period=14)
    summary = res["summary"]
    assert summary["n_trades"] == 1
    assert summary["wins"] == 1  # because ramp should hit TP
    assert summary["winrate"] == 1.0
    assert res["trades"][0]["reason"] == "tp"
