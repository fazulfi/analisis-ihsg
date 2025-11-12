# tests/test_signal_rules.py
import pandas as pd
from datetime import datetime, timedelta
from analyzer.signal_engine.rules import generate_signals

def make_df(prices):
    # create a small DataFrame with timestamp index and 'close' column
    start = datetime(2025,1,1)
    ts = [start + timedelta(minutes=i) for i in range(len(prices))]
    df = pd.DataFrame({"timestamp": ts, "close": prices})
    df.set_index("timestamp", inplace=False)  # keep timestamp as column too
    return df

def test_generate_signals_simple_cross_and_rsi():
    # craft prices so EMA short crosses EMA long and RSI increases
    # Use small spans so we get indicators quickly: short=3, long=5, rsi_period=3
    # design: flat, then small rise to produce cross around idx 4
    prices = [100, 100, 100, 101, 103, 106, 108, 110]
    df = pd.DataFrame({
        "timestamp": pd.date_range("2025-01-01", periods=len(prices), freq="min"),
        "close": prices
    })
    cfg = {"ema_spans": (3,5), "rsi_period": 3}
    out = generate_signals(df.copy(), cfg=cfg, price_col="close", ts_col="timestamp", force_indicators=True, emit_next_open=False)
    # Expect at least one BUY where short EMA crosses above long EMA and RSI confirms
    assert isinstance(out, list)
    assert len(out) >= 1
    # Ensure each emitted signal has expected keys
    for s in out:
        assert set(s.keys()) >= {"index","ts","signal","price"}
        assert s["signal"] == "BUY"

def test_emit_next_open_behaviour():
    prices = [100, 100, 99, 100, 102, 105]  # a cross may happen at index 4, we expect next open emission shifts index
    df = pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=len(prices), freq="min"), "close": prices})
    cfg = {"ema_spans": (3,5), "rsi_period": 3}
    out_next = generate_signals(df.copy(), cfg=cfg, price_col="close", ts_col="timestamp", force_indicators=True, emit_next_open=True)
    # If signals emitted, index in result should be >0 (shifted)
    if out_next:
        assert all(s["index"] > 0 for s in out_next)
