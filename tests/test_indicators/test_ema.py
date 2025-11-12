# tests/test_indicators/test_ema.py
import math
from analyzer.indicators.ema import ema

def test_ema_basic_seed_and_length():
    prices = [1,2,3,4,5,6,7,8,9,10]
    period = 3
    out = ema(prices, period)
    # panjang harus sama
    assert len(out) == len(prices)
    # index period-1 harus terisi dengan SMA seed
    expected_sma = sum(prices[:period]) / period
    assert out[period-1] == expected_sma
    # setelah seed, nilai seharusnya numeric (float) dan finite
    for v in out[period-1:]:
        assert v is None or (isinstance(v, float) and math.isfinite(v))
    # sebelum seed harus None
    for i in range(period-1):
        assert out[i] is None

def test_ema_known_values():
    # sanity check: monotonic input harus menghasilkan EMA yang naik
    prices = [10, 11, 12, 13, 14, 15]
    out = ema(prices, 3)
    # after seed, should be increasing
    numeric = [v for v in out if v is not None]
    assert len(numeric) > 0
    assert all(numeric[i] <= numeric[i+1] for i in range(len(numeric)-1))
