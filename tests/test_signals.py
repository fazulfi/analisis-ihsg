# tests/test_signals.py
import math
import pandas as pd
from analyzer.indicators.ema import ema
from analyzer.indicators.rsi import rsi
from analyzer.indicators import ema_cross_buy, rsi_buy_condition
from analyzer.signal_engine.rules import generate_signals

def test_ema_basic_seed_and_values():
    prices = [1,2,3,4,5,6,7,8,9,10]
    out = ema(prices, 3)
    # panjang sama dengan input
    assert len(out) == len(prices)
    # seed (index 2) harus berisi SMA periode pertama = (1+2+3)/3 = 2.0
    assert out[2] == 2.0
    # nilai setelah seed harus numeric dan meningkat
    assert out[3] == 3.0
    assert out[-1] == 9.0

def test_rsi_uptrend_approaches_100():
    # naik terus -> RSI mendekati 100 setelah konvergensi
    prices = list(range(50, 101))  # terus naik
    r = rsi(prices, 14)
    # panjang sama
    assert len(r) == len(prices)
    # value terakhir harus tinggi (mendekati 100)
    assert r[-1] is not None
    assert float(r[-1]) > 90.0

def test_steady_no_buy():
    # harga datar -> tidak ada cross dan tidak ada BUY
    prices = [100.0] * 12
    # low-level checks
    ef = ema(prices, 3)
    es = ema(prices, 5)
    crosses = ema_cross_buy(ef, es)
    assert all(x is False for x in crosses)
    r = rsi(prices, 14)
    rcond = rsi_buy_condition(r)
    assert all(x is False for x in rcond)
    # generate_signals on DataFrame should return no BUY
    df = pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=len(prices), freq="min"),
                       "close": prices})
    out = generate_signals(df.copy(), cfg={"ema_spans": (3,5), "rsi_period": 14},
                           price_col="close", ts_col="timestamp", force_indicators=True, emit_next_open=False)
    assert isinstance(out, list)
    assert len(out) == 0

def test_cross_and_rsi_produces_at_least_one_buy():
    # desain synthetic yang memicu fast EMA cross above slow dan RSI naik
    prices = [100,100,100,101,103,106,108,110]
    df = pd.DataFrame({
        "timestamp": pd.date_range("2025-01-01", periods=len(prices), freq="min"),
        "close": prices
    })
    cfg = {"ema_spans": (3,5), "rsi_period": 3}
    signals = generate_signals(df.copy(), cfg=cfg, price_col="close", ts_col="timestamp",
                               force_indicators=True, emit_next_open=False)
    # minimal satu BUY diharapkan
    assert isinstance(signals, list)
    assert len(signals) >= 1
    for s in signals:
        assert s["signal"] == "BUY"
        assert "ts" in s and "price" in s
