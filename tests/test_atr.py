# tests/test_atr.py
import pandas as pd
import numpy as np
from analyzer.indicators import add_atr

def sample_ohlcv_volatility_change():
    # buat data low volatility lalu spike volatilitas
    prices_close = list(np.linspace(100, 101, 30))  # relatif tenang
    highs = [p + 0.5 for p in prices_close]
    lows = [p - 0.5 for p in prices_close]
    # lalu tambahkan beberapa bar dengan volatilitas naik
    extra_close = [101, 103, 98, 107, 100]
    extra_highs = [c + 3 for c in extra_close]
    extra_lows = [c - 3 for c in extra_close]
    close = prices_close + extra_close
    high = highs + extra_highs
    low = lows + extra_lows
    times = pd.date_range("2025-01-01", periods=len(close), freq="min")
    df = pd.DataFrame({"close": close, "high": high, "low": low}, index=times)
    return df

def test_add_atr_creates_column_and_increases_on_spike():
    df = sample_ohlcv_volatility_change()
    out = add_atr(df.copy(), period=14)
    col = "atr_14"
    assert col in out.columns
    # setelah warmup (index > period) ATR tidak NaN
    idx = min(len(out)-1, 14*2)
    assert pd.notna(out[col].iloc[idx])
    # ATR harus naik saat spike volatilitas (bandingkan sebelum dan setelah spike)
    before = out[col].iloc[25]   # relatif tenang
    after = out[col].iloc[-1]    # setelah spike
    assert after > before
