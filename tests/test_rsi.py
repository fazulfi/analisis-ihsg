# tests/test_rsi.py
import pandas as pd
import numpy as np
from analyzer.indicators import add_rsi

def gen_uptrend(n=100):
    # harga naik linear -> RSI cenderung tinggi
    prices = [100 + i * 0.5 for i in range(n)]
    times = pd.date_range("2025-01-01", periods=n, freq="min")
    df = pd.DataFrame({"close": prices}, index=times)
    return df

def gen_flat(n=50):
    # harga flat -> RSI = ~50 after warmup
    prices = [100.0] * n
    times = pd.date_range("2025-01-01", periods=n, freq="min")
    df = pd.DataFrame({"close": prices}, index=times)
    return df

def test_add_rsi_creates_column_and_range():
    df = gen_uptrend(100)
    out = add_rsi(df.copy(), period=14)
    assert "rsi_14" in out.columns
    # after warmup (2*period) it should be non-null and within 0-100
    idx = min(len(out)-1, 14*2)
    assert pd.notna(out["rsi_14"].iloc[idx])
    assert 0.0 <= out["rsi_14"].iloc[idx] <= 100.0

def test_rsi_reacts_to_price_change():
    df = gen_uptrend(100)
    out = add_rsi(df.copy(), period=14)
    # RSI should be increasing towards the end for steady uptrend
    rsi_series = out["rsi_14"].dropna()
    assert rsi_series.iloc[-1] > rsi_series.iloc[len(rsi_series)//2]

def test_flat_series_rsi_around_50():
    df = gen_flat(60)
    out = add_rsi(df.copy(), period=14)
    # after warmup, RSI should be ~50 for flat prices
    rsi_series = out["rsi_14"].dropna()
    assert abs(rsi_series.iloc[-1] - 50.0) <= 1.0  # tolerance 1.0
