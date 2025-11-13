from indicators.rounding import round_price_to_tick, enforce_tick_rounding_on_signals

def test_round_price_floor_5():
    assert round_price_to_tick(977, 5, "floor") == 975.0

def test_round_price_ceil_5():
    assert round_price_to_tick(1063, 5, "ceil") == 1065.0

def test_enforce_rounding_on_rows():
    rows = [
        {"index": 1, "signal_type": "BUY", "sl_price": 977.0, "tp_price": 1063.0},
        {"index": 2, "signal_type": "SELL", "sl_price": 1063.0, "tp_price": 977.0},
    ]
    new_rows, warnings = enforce_tick_rounding_on_signals(rows, tick_size=5)
    assert new_rows[0]['sl_price_rounded'] == 975.0
    assert new_rows[0]['tp_price_rounded'] == 1065.0
    # SELL: sl uses ceil, tp uses floor
    assert new_rows[1]['sl_price_rounded'] == 1065.0
    assert new_rows[1]['tp_price_rounded'] == 975.0
    assert warnings == []
