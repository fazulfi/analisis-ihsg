from indicators.rounding import round_price_to_tick, enforce_tick_rounding_on_signals

def test_floor_and_ceil_5():
    assert round_price_to_tick(977, 5, "floor") == 975.0
    assert round_price_to_tick(1063, 5, "ceil") == 1065.0

def test_enforce_rounding_and_no_round_on_invalid():
    rows = [{"index":1,"signal_type":"BUY","sl_price":977,"tp_price":1063}]
    new_rows, warnings = enforce_tick_rounding_on_signals(rows, tick_size=5)
    assert new_rows[0]['sl_price_rounded'] == 975.0
    assert new_rows[0]['tp_price_rounded'] == 1065.0
    # invalid tick -> no rounding + warning
    new_rows2, warnings2 = enforce_tick_rounding_on_signals(rows, tick_size=0)
    assert new_rows2[0]['sl_price_rounded'] == 977
    assert len(warnings2) >= 1
