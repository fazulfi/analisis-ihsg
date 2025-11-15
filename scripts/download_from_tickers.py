#!/usr/bin/env python3
import argparse
import yfinance as yf
import pandas as pd
from datetime import datetime
import os

# --------------------------------------------------------
# Normalize DataFrame column names (flatten MultiIndex)
# --------------------------------------------------------
def normalize_columns(df):
    new_cols = []
    for c in df.columns:
        if isinstance(c, tuple):
            new_cols.append("_".join([str(x) for x in c if x]))
        else:
            new_cols.append(str(c))
    df.columns = new_cols
    return df


# --------------------------------------------------------
# Save dataframe with fixed column names
# --------------------------------------------------------
def save_clean(df, outfile):
    df = df.reset_index(drop=True)

    # force standard ordering if exists
    cols = df.columns.tolist()

    preferred = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    final_cols = [c for c in preferred if c in cols] + [c for c in cols if c not in preferred]

    df = df[final_cols]
    df.to_csv(outfile, index=False)
    print(f"[OK] wrote {outfile}")


# --------------------------------------------------------
# Main downloader
# --------------------------------------------------------
def download_one(ticker, start, end, outdir):
    print(f"Downloading {ticker} start={start} end={end}")

    df = yf.download(
        ticker,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=True,
        progress=False
    )

    if df is None or df.empty:
        print(f"[WARN] {ticker} returned empty data.")
        return False

    df = df.reset_index()               # Ensure Date column
    df = normalize_columns(df)          # Flatten multiindex

    outfile = os.path.join(outdir, f"{ticker}.csv")
    save_clean(df, outfile)
    return True


# --------------------------------------------------------
# Read tickers from file
# --------------------------------------------------------
def load_tickers_from_file(path):
    tickers = []
    with open(path, "r") as f:
        for line in f:
            t = line.strip()
            if t:
                tickers.append(t)
    return tickers


# --------------------------------------------------------
# CLI
# --------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers-file", help="File berisi daftar tickers (1 per baris)")
    parser.add_argument("--tickers", nargs="*", help="Daftar tickers langsung")
    parser.add_argument("--outdir", default="data/tickers", help="Output folder")
    parser.add_argument("--start", default="2015-01-01")
    parser.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # Ambil tickers
    if args.tickers_file:
        tickers = load_tickers_from_file(args.tickers_file)
    else:
        tickers = args.tickers or []

    if not tickers:
        print("ERROR: tickers kosong.")
        return

    print(f"Downloading tickers: {tickers}")
    print(f"Date range: {args.start} → {args.end}")

    ok = 0
    fail = 0

    for t in tickers:
        try:
            if download_one(t, args.start, args.end, args.outdir):
                ok += 1
            else:
                fail += 1
        except Exception as e:
            print(f"[ERROR] {t}: {e}")
            fail += 1

    print("\nSummary:")
    print(f"  downloaded: {ok}")
    print(f"  failed    : {fail}")


if __name__ == "__main__":
    main()
