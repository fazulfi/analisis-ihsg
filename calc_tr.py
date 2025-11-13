#!/usr/bin/env python3
"""
calc_tr.py
Hitung True Range (TR) dan tambahkan kolom 'tr' ke CSV input.

Usage:
  python calc_tr.py DATA/<TICKER>.csv OUTPUT/<TICKER>_with_tr.csv

Exit codes:
  0 - success
  2 - usage / file missing
  3 - read/write error / pandas not installed
"""
import sys, os
from pathlib import Path

try:
    import pandas as pd
except Exception:
    pd = None

def compute_tr(df):
    # Pastikan kolom lower-case
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # ensure needed columns
    for c in ("high", "low", "close"):
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    # coerce numeric
    for c in ("high", "low", "close"):
        df[c] = pd.to_numeric(df[c], errors='raise')

    # shift prev close
    df['prev_close'] = df['close'].shift(1)

    # candidate 1: high - low
    df['cand1'] = df['high'] - df['low']

    # candidate 2: abs(high - prev_close)
    df['cand2'] = (df['high'] - df['prev_close']).abs()

    # candidate 3: abs(low - prev_close)
    df['cand3'] = (df['low'] - df['prev_close']).abs()

    # For first row, prev_close is NaN, cand2 & cand3 become NaN; we want TR = cand1 for first row
    # Use combine to fill NaN with cand1 where necessary
    df['tr'] = df[['cand1','cand2','cand3']].max(axis=1)

    # Ensure non-negative
    if (df['tr'] < 0).any():
        raise ValueError("Computed TR has negative values — check inputs")

    # cleanup helper cols, keep tr
    df = df.drop(columns=['prev_close','cand1','cand2','cand3'])
    return df

def main():
    if len(sys.argv) < 3:
        print("Usage: python calc_tr.py <input.csv> <output.csv>")
        sys.exit(2)

    inpath = Path(sys.argv[1])
    outpath = Path(sys.argv[2])

    if not inpath.exists():
        print(f"ERROR: input file not found: {inpath}")
        sys.exit(2)

    if pd is None:
        print("ERROR: pandas not installed. pip install pandas")
        sys.exit(3)

    try:
        df = pd.read_csv(inpath)
    except Exception as e:
        print("ERROR reading CSV:", e)
        sys.exit(3)

    try:
        df_out = compute_tr(df)
    except Exception as e:
        print("ERROR computing TR:", e)
        sys.exit(3)

    try:
        outpath.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(outpath, index=False)
    except Exception as e:
        print("ERROR writing output CSV:", e)
        sys.exit(3)

    print("OK: TR computed and saved to", outpath)
    sys.exit(0)

if __name__ == "__main__":
    main()
