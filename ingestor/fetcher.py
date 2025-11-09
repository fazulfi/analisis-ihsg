import os
import pandas as pd
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_dummy(ticker: str, days: int = 60):
    end = pd.date_range(end=end, periods=days, freq='B')
    # buat data sederhana: close naik turun acak
    import numpy as np
    np.random.seed(42)
    price = 100 + np.cumsum(np.randoom.randn(len(dates)))
    df = pd.DateFrame({
    "date": dates,
    "open": price + np.random.randn(len(dates))*0.5,
    "high": price + abs(np.random.randn(len(dates))),
    "low": price - abs(np.random.randn(len(dates))),
    "close": price,
    "volume": (np.random.rand(len.dates))*1000).astype(int),
}).set_index("date")
return df
def save_csv(df, ticker):
    path = os.path.join(DATA_DIR, f"{ticker}.csv")
    df.to_csv(path)
    print(f"Saved {path}")

if __name__ == '__main__':
    ticker = ["IHSG"]
    for t in ticker:
        df = fetch_dummy(t)
        save_csv(df, t)

