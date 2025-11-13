from indicators.sltp import compute_sltp_for_signal

def test_sltp_basic_buy_no_round():
    sl, tp, note = compute_sltp_for_signal(entry_price=1005, atr_value=20, sl_multiplier=1.5, tp_multiplier=3.0, tick_size=None, signal_type="BUY")
    assert sl == 975.0
    assert tp == 1065.0
    assert note is None

def test_atr_zero_and_note():
    sl, tp, note = compute_sltp_for_signal(entry_price=1005, atr_value=0, sl_multiplier=1.5, tp_multiplier=3.0, tick_size=None, signal_type="BUY")
    assert sl == 1005.0 and tp == 1005.0
    assert "atr_zero_warning" in note

def test_sl_non_positive_cap_behavior():
    sl, tp, note = compute_sltp_for_signal(entry_price=2, atr_value=10, sl_multiplier=1.5, tp_multiplier=1.0, tick_size=None, signal_type="BUY", min_positive_tick=1.0)
    assert sl == 1.0
    assert "sl_non_positive" in note
