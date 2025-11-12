# tests/test_indicators/test_rsi.py
import math
from analyzer.indicators.rsi import rsi

def test_rsi_length_and_seed():
    prices = [1,2,3,4,5,6,7,8,9,10]
    period = 3
    out = rsi(prices, period)
    assert len(out) == len(prices)
    # first period-1 positions None
    for i in range(period-1):
        assert out[i] is None
    # index period should be numeric
    assert out[period] is not None
    assert isinstance(out[period], float)

def test_rsi_uptrend_approaches_100():
    # steadily rising prices
    prices = [100 + i for i in range(200)]
    period = 14
    out = rsi(prices, period)
    numeric = [v for v in out if v is not None]
    # last value should be > 90 (approaches 100)
    assert numeric[-1] > 90.0

def test_rsi_downtrend_approaches_0():
    prices = [200 - i for i in range(200)]
    period = 14
    out = rsi(prices, period)
    numeric = [v for v in out if v is not None]
    # last value should be < 10 (approaches 0)
    assert numeric[-1] < 10.0
