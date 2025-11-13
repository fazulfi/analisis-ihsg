import pandas as pd
from signal_engine.signals import generate_signals

def make_df_from_close(close_list):
    return pd.DataFrame({"date":[f"2025-01-{i+1:02d}" for i in range(len(close_list))],
                         "open": close_list,
                         "high": [c+1 for c in close_list],
                         "low": [c-1 for c in close_list],
                         "close": close_list,
                         "volume":[100]*len(close_list)})

def test_generate_signals_basic_buy():
    # Build a series that ensures EMA_short starts below EMA_long (diff negative),
    # then crosses above as prices rise.
    # Start with a higher plateau so long EMA > short EMA initially, then drop then ramp.
    close = [
        120,120,120,   # initial plateau (make long EMA > short EMA)
        100,100,100,   # drop so short tracks lower quickly
        101,102,103,104,105,106,107,108
    ]
    df = make_df_from_close(close)
    # set rsi_buy_thresh high so RSI check does not block BUY on this strong uptrend
    sigs = generate_signals(df, params={"ema_short":3,"ema_long":5,"rsi_period":5,"rsi_buy_thresh":100,"rsi_sell_thresh":30,"min_signal_distance":1})
    # expect at least one BUY (crossover up)
    assert any(s["signal_type"]=="BUY" for s in sigs), f"expected BUY signals but got: {sigs}"

def test_generate_signals_insufficient_data():
    close = [100,101]  # too short for EMA/RSI
    df = make_df_from_close(close)
    sigs = generate_signals(df)
    assert sigs == []

def test_generate_signals_min_distance():
    # make crosses that could trigger multiple signals but min_distance prevents them
    # produce alternating small ups so cross happens frequently
    close = [100,101,100,101,100,101,100,101,100,101, 102,103,104]
    df = make_df_from_close(close)
    sigs = generate_signals(df, params={"ema_short":2,"ema_long":3,"rsi_period":3,"rsi_buy_thresh":80,"min_signal_distance":3})
    # ensure successive signals are at least 3 bars apart
    if len(sigs) >= 2:
        for a,b in zip(sigs, sigs[1:]):
            assert b["index"] - a["index"] >= 3
