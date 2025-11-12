# tests/test_indicators/test_ema_cross.py
from analyzer.indicators import ema_cross_buy

def test_ema_cross_buy_basic():
    ef = [1, 1, 2, 3]
    es = [1, 1.5, 1.8, 2.5]
    out = ema_cross_buy(ef, es)
    # length must match input (min length)
    assert len(out) == 4
    # Only index 2 should be True (0-based)
    assert out == [False, False, True, False]

def test_ema_cross_with_none_and_nan():
    import math
    ef = [None, 1.0, 2.0, 3.0]
    es = [1.0, 1.0, float('nan'), 2.5]
    out = ema_cross_buy(ef, es)
    # missing values should cause False results
    assert out == [False, False, False, False]
