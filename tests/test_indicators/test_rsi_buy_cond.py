# tests/test_indicators/test_rsi_buy_cond.py
from analyzer.indicators.rsi import rsi_buy_condition

def test_rsi_buy_basic():
    rsi = [None, 28, 31, 32]
    out = rsi_buy_condition(rsi)
    assert out == [False, False, True, True]

def test_rsi_buy_with_nan_and_none():
    import math
    rsi = [None, 28.0, float('nan'), 35.0, 36.0]
    out = rsi_buy_condition(rsi)
    # index 2 is nan -> False; index 3 prev is nan -> False; index 4 prev is 35.0 so check 36>30 and 36>35 -> True
    assert out == [False, False, False, False, True]
