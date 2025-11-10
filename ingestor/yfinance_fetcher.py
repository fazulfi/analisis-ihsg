# ingestor/yfinance_fetcher.py
import os
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data", "raw")
DB_PATH = os.path.join(ROOT, "data", "historical.db")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def fetch_to_csv(ticker: str, period="1y", interval="1d"):
    print(f"[fetch] {ticker} period={period} interval={interval}")
    df = yf.Ticker(ticker).history(period=period, interval=interval)
    if df is None or df.empty:
        print(f"[fetch] WARNING: no data for {ticker}")
        return None
    df.index.name = "date"
    path = os.path.join(DATA_DIR, f"{ticker}.csv")
    df.to_csv(path)
    print("[fetch] Saved", path)
    return df

def fetch_to_sqlite(ticker: str, period="1y", interval="1d"):
    df = fetch_to_csv(ticker, period, interval)
    if df is None:
        return
    engine = create_engine(f"sqlite:///{DB_PATH}")
    df2 = df.copy()
    df2["ticker"] = ticker
    df2.reset_index(inplace=True)
    # convert date column to iso string for sqlite portability
    df2["date"] = df2["date"].astype(str)
    df2.to_sql("prices", engine, if_exists="append", index=False)
    print("[fetch] Appended to sqlite:", DB_PATH)

def fetch_many(tickers, period="1y", interval="1d"):
    for t in tickers:
        try:
            fetch_to_sqlite(t, period=period, interval=interval)
        except Exception as e:
            print("[fetch] error", t, e)
