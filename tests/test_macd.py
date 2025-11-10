# tests/test_macd.py
import pandas as pd
import numpy as np
from analyzer.indicators import add_macd

def sample_price_with_trend_change():
    """
    Buat seri harga yang awalnya turun lalu naik tajam sehingga fast EMA
    akan cross above slow EMA -> menghasilkan perubahan tanda pada histogram.
    """
    # turun (100 -> 80), lalu naik cepat (80 -> 120)
    part1 = list(np.linspace(100, 80, 40))   # turun perlahan
    part2 = list(np.linspace(80, 120, 40))   # naik cepat
    prices = part1 + part2
    times = pd.date_range("2025-01-01", periods=len(prices), freq="min")
    df = pd.DataFrame({"close": prices}, index=times)
    return df

def test_add_macd_creates_columns_and_hist_sign_change():
    df = sample_price_with_trend_change()
    out = add_macd(df.copy(), fast=5, slow=12, signal=9)

    # kolom ada
    assert "macd" in out.columns
    assert "macd_signal" in out.columns
    assert "macd_hist" in out.columns

    hist = out["macd_hist"].dropna()
    assert len(hist) > 0

    # histogram harus punya nilai positif dan negatif (perubahan tanda)
    has_pos = (hist > 0).any()
    has_neg = (hist < 0).any()
    assert has_pos and has_neg, "Expected macd_hist to have both positive and negative values"

    # cek ada titik di mana tanda berubah (cross)
    sign_series = np.sign(hist)
    # diff of sign: non-zero indicates a sign change between adjacent points
    sign_changes = np.abs(np.diff(sign_series))
    assert (sign_changes > 0).any(), "Expected at least one sign change in macd_hist"
