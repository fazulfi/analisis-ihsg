import pandas as pd
from analyzer.signals import add_indicators

def test_add_indicators():
    dates = pd.date_range("2023-01-01", periods=20)
    df = pd.DataFrame({"close": list(range(20))}, index=dates)
    df2 = add_indicators(df.copy())
    assert 'ema_short' in df2.columns
    assert 'rsi' in df2.columns
