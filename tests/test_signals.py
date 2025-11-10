# tests/test_signals.py
import pandas as pd
from analyzer.signals import generate_ema_signals

def simple_price_with_crossover():
    # buat harga yang awalnya flat lalu naik sehingga short EMA cross up long EMA
    prices = [100]*10 + list(range(100, 120))  # flat then ramp up
    times = pd.date_range("2025-01-01", periods=len(prices), freq="min")
    df = pd.DataFrame({"close": prices}, index=times)
    return df

def test_generate_ema_signals_produces_buy_when_crossover():
    df = simple_price_with_crossover()
    out = generate_ema_signals(df.copy(), short=3, long=8)
    # pastikan kolom dibuat
    assert f"ema_3" in out.columns
    assert f"ema_8" in out.columns
    assert "signal" in out.columns
    # ada minimal 1 buy
    assert (out["signal"] == "buy").any()
    # cek posisi pertama buy index terjadi setelah flat -> ramp
    buy_idx = out.index[out["signal"] == "buy"][0]
    assert buy_idx > out.index[0]
