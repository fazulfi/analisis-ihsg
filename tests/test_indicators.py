# tests/test_indicators.py
import pandas as pd
import numpy as np
from analyzer import indicators

def sample_price_df(n=100, start=100.0, step=0.1):
    """Generate DataFrame sederhana yang naik bertahap (deterministik)."""
    prices = [start + i * step for i in range(n)]
    times = pd.date_range("2025-01-01", periods=n, freq="min")
    df = pd.DataFrame({"close": prices}, index=times)
    return df

def test_add_ema_creates_columns_and_warms_up():
    df = sample_price_df(n=100)
    spans = (5, 10, 20)
    out = indicators.add_ema(df.copy(), spans=spans)
    # kolom ada
    for s in spans:
        assert f"ema_{s}" in out.columns
    # setelah warmup (lebih dari span), nilai tidak NaN
    for s in spans:
        col = f"ema_{s}"
        # ambil index yang jauh setelah periode (misal index ke- (span*2))
        idx = min(len(out) - 1, s * 2)
        assert pd.notna(out[col].iloc[idx]), f"{col} is NaN at warmup index"
    # nilai EMA harus mengikuti arah harga (karena price monoton naik)
    assert out["ema_5"].iloc[-1] < out["close"].iloc[-1]
    assert out["ema_20"].iloc[-1] < out["ema_5"].iloc[-1]

def test_add_ema_invalid_pricecol_raises():
    df = sample_price_df(n=10)
    try:
        indicators.add_ema(df, spans=(5,), price_col="nonexistent")
        raised = False
    except ValueError:
        raised = True
    assert raised, "Expected ValueError when price_col missing"
