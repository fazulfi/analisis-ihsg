# tests/test_signal_rules.py
import pandas as pd
import numpy as np
from analyzer.signals import generate_signals

def create_series_with_valid_confirmation():
    """
    Generate a price series that:
    - has a clear short EMA crossing above long EMA,
    - RSI remains below overbought (not >70),
    - MACD histogram becomes positive after crossover.
    We'll craft a short sequence so the test is deterministic.
    """
    # flat then ramp up
    prices = [100.0] * 10 + list(np.linspace(100.0, 110.0, 20))
    times = pd.date_range("2025-01-01", periods=len(prices), freq="min")
    df = pd.DataFrame({"close": prices}, index=times)
    # Add fake high/low for ATR (optional)
    df["high"] = df["close"] + 0.2
    df["low"] = df["close"] - 0.2
    return df

def test_generate_signals_produces_buy():
    df = create_series_with_valid_confirmation()
    cfg = {"short": 3, "long": 8, "rsi_period": 14, "rsi_overbought": 70.0, "macd": {"fast":3, "slow":8, "signal":5}}
    out = generate_signals(df.copy(), config=cfg, force_indicators=True)
    # expect at least one buy
    assert (out["signal"] == "buy").any(), "Expected at least one buy signal"
    # check that buy rows also have ema columns computed
    assert f"ema_{cfg['short']}" in out.columns
    assert f"ema_{cfg['long']}" in out.columns
    # confirm that every 'buy' row satisfies the EMA crossover condition
    buystamps = out.index[out["signal"] == "buy"]
    assert len(buystamps) > 0
    for ts in buystamps:
        row = out.loc[ts]
        # short ema should be >= long ema at buy (it crossed up)
        assert row[f"ema_{cfg['short']}"] >= row[f"ema_{cfg['long']}"]
