import os
import pandas as pd
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_dummy(ticker: str, days: int = 60):
    end = datetime.today()
    dates = pd.date_range(end=end, periods=days, freq="B")

    import numpy as np
    np.random.seed(42)

    price = 100 + np.cumsum(np.random.randn(len(dates)))

    df = pd.DataFrame({
        "date": dates,
        "open": price + np.random.randn(len(dates)) * 0.5,
        "high": price + abs(np.random.randn(len(dates))),
        "low": price - abs(np.random.randn(len(dates))),
        "close": price,
        "volume": (np.random.rand(len(dates)) * 1000).astype(int)
    })

    df.set_index("date", inplace=True)
    return df

def save_csv(df, ticker):
    path = os.path.join(DATA_DIR, f"{ticker}.csv")
    df.to_csv(path)
    print(f"Saved {path}")

if __name__ == "__main__":
    tickers = ["IHSG"]
    for t in tickers:
        df = fetch_dummy(t)
        save_csv(df, t)
