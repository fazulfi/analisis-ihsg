# tests/test_add_all_indicators.py
import pandas as pd
from analyzer.indicators import add_all_indicators

def sample_ohlcv(n=100):
    times = pd.date_range("2025-01-01", periods=n, freq="min")
    close = [100 + i*0.1 for i in range(n)]
    high = [c + 0.5 for c in close]
    low = [c - 0.5 for c in close]
    open_ = [c - 0.05 for c in close]
    vol = [1000 + i for i in range(n)]
    df = pd.DataFrame({"open": open_, "high": high, "low": low, "close": close, "volume": vol}, index=times)
    return df

def test_add_all_indicators_creates_expected_columns():
    df = sample_ohlcv(100)
    cfg = {"ema_spans": (5, 10), "rsi_period": 14, "macd": {"fast":5, "slow":12, "signal":9}, "atr_period": 14}
    out = add_all_indicators(df.copy(), cfg, force=True)
    # check EMA cols
    assert "ema_5" in out.columns and "ema_10" in out.columns
    # RSI
    assert "rsi_14" in out.columns
    # MACD
    assert "macd" in out.columns and "macd_signal" in out.columns and "macd_hist" in out.columns
    # ATR
    assert "atr_14" in out.columns
