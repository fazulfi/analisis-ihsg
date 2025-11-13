import pandas as pd
from signal_engine.integration import attach_atr_and_entry_to_signals

def make_df_with_atr(n=30):
    # build a simple increasing series and a fake 'atr' (non-zero after idx 14)
    dates = pd.date_range("2025-01-01", periods=n).astype(str)
    data = {
        "date": dates,
        "open": [1000 + i for i in range(n)],
        "high": [1005 + i for i in range(n)],
        "low": [995 + i for i in range(n)],
        "close": [1002 + i for i in range(n)],
        "volume": [10000 + i*100 for i in range(n)],
    }
    df = pd.DataFrame(data)
    # compute a fake atr:  for simplicity, atr = i*1.0 for i>=14 else NaN
    atr = [float(i) if i >= 14 else float('nan') for i in range(n)]
    df["atr"] = atr
    return df

def test_attach_atr_entry_normal():
    df = make_df_with_atr(30)
    # create a fake BUY signal at index 20
    signals = [{"index": 20, "signal_type": "BUY", "date": df.loc[20, "date"]}]
    cfg = {"entry_price_source": "close"}
    out = attach_atr_and_entry_to_signals(df, signals, cfg=cfg)
    assert len(out) == 1
    s = out[0]
    assert s["atr_value"] == float(20)        # atr at idx20 = 20.0 in our fake df
    # entry_price should be close at idx20
    assert s["entry_price"] == float(df.loc[20, "close"])
    assert s["note"] is None

def test_attach_atr_next_open_and_insufficient():
    df = make_df_with_atr(5)  # length < 14 so atr all NaN
    # signal at last index (no next bar)
    signals = [{"index": 4, "signal_type": "BUY", "date": df.loc[4, "date"]}]
    cfg = {"entry_price_source": "next_open"}
    out = attach_atr_and_entry_to_signals(df, signals, cfg=cfg)
    s = out[0]
    # atr is NaN -> insufficient_data_for_atr
    assert s["atr_value"] is None
    assert s["note"] == "insufficient_data_for_atr"
