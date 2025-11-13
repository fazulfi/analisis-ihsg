import pandas as pd
from signal_engine.postprocess import enforce_single_open_signal

def make_df(highs, lows, dates=None):
    n = len(highs)
    if dates is None:
        dates = [f"2025-01-{i+1:02d}" for i in range(n)]
    return pd.DataFrame({
        "date": dates,
        "open": [0]*n,
        "high": highs,
        "low": lows,
        "close": [0]*n,
        "volume": [0]*n
    })

def test_postprocess_blocks_second_signal_before_close():
    # create simple bars where first signal at idx=1 will not close by idx=3 -> second at idx=2 must be skipped
    highs = [100, 105, 104, 103, 110]  # TP for first at 110 will only hit at idx=4
    lows  = [95,  95,  95,  95,  95]
    df = make_df(highs, lows)
    # two fake signals: first at idx=1, second at idx=2
    attached = [
        {"index": 1, "signal_type": "BUY", "entry_price": 100.0, "atr_value": 2.0},
        {"index": 2, "signal_type": "BUY", "entry_price": 100.0, "atr_value": 2.0},
    ]
    # use multipliers so TP = entry + 3*atr = 106 -> will hit at idx=4 only if set larger; here choose tp_mult=5 to push hit at index 4
    cfg = {"sl_multiplier": 1.5, "tp_multiplier": 5.0}
    kept, skipped = enforce_single_open_signal(attached, df, cfg=cfg)
    # first must be kept, second skipped
    assert len(kept) == 1
    assert kept[0]["index"] == 1
    assert len(skipped) == 1
    assert "skipped_open_trade" in skipped[0].get("note", "")

def test_postprocess_allows_signal_after_close():
    # first signal closes at idx=2, second at idx=3 -> both allowed
    highs = [100, 106, 100, 108]  # idx1 high >= tp_raw -> close at idx=1/2
    lows  = [95,  95,  95,  95]
    df = make_df(highs, lows)
    attached = [
        {"index": 1, "signal_type": "BUY", "entry_price": 100.0, "atr_value": 2.0},
        {"index": 3, "signal_type": "BUY", "entry_price": 100.0, "atr_value": 2.0},
    ]
    cfg = {"sl_multiplier": 1.5, "tp_multiplier": 2.5}
    kept, skipped = enforce_single_open_signal(attached, df, cfg=cfg)
    assert len(kept) == 2
    assert len(skipped) == 0
