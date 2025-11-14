#!/usr/bin/env python3
"""
scripts/make_sample_csv.py
Usage:
  python scripts/make_sample_csv.py BBRI  --rows 100 --out data/BBRI.csv

Generates a simple synthetic price series with header:
date,open,high,low,close,volume
Date format: YYYY-MM-DD
"""
import sys, os
from datetime import datetime, timedelta
import random
import csv

def make_price_series(rows=100, start_price=1000.0):
    prices = []
    price = float(start_price)
    for i in range(rows):
        # random walk small step
        step = random.uniform(-0.02, 0.02)  # +/- 2%
        new_close = round(price * (1 + step), 3)
        high = max(new_close, price) * (1 + random.uniform(0,0.005))
        low = min(new_close, price) * (1 - random.uniform(0,0.005))
        openp = price
        volume = random.randint(10000, 1000000)
        prices.append((openp, round(high,3), round(low,3), new_close, volume))
        price = new_close
    return prices

def write_csv(ticker="TICKER", rows=100, outpath="data/TICKER.csv", start_date="2020-01-01"):
    os.makedirs(os.path.dirname(outpath) or ".", exist_ok=True)
    dt = datetime.strptime(start_date, "%Y-%m-%d")
    series = make_price_series(rows=rows, start_price=1000.0)
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "open", "high", "low", "close", "volume"])
        for i, (openp, high, low, close, vol) in enumerate(series):
            row_date = (dt + timedelta(days=i)).strftime("%Y-%m-%d")
            w.writerow([row_date, openp, high, low, close, vol])
    print(f"Wrote {outpath} rows={rows}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("ticker", help="ticker name (used for output filename)")
    p.add_argument("--rows", type=int, default=100)
    p.add_argument("--out-dir", default="data")
    p.add_argument("--start", default="2015-01-01")
    args = p.parse_args()
    out = os.path.join(args.out_dir, f"{args.ticker}.csv")
    write_csv(ticker=args.ticker, rows=args.rows, outpath=out, start_date=args.start)
