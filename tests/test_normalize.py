import pandas as pd
from ingestor.storage import normalize_ohlcv

def test_normalize_from_df_and_cols():
    data = {
        'Date': ['2025-11-07 09:00', '2025-11-07 10:00'],
        'Open_Price': [10, 11],
        'High': [11, 12],
        'Low': [9, 10],
        'Adj Close': [10.5, 11.2],
        'Volume': [100, 200],
    }
    df = pd.DataFrame(data)
    out = normalize_ohlcv(df)
    assert list(out.columns) == ['open','high','low','close','volume']
    assert isinstance(out.index, pd.DatetimeIndex)
    assert out.shape[0] == 2
