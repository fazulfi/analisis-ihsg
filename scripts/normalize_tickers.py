#!/usr/bin/env python3
"""
Normalize ticker CSV files so pipeline sees consistent columns:
 - timestamp (YYYY-MM-DD)
 - open, high, low, close, volume
Writes normalized CSV back to the same path by default (makes .bak if --backup).
"""

import argparse
import glob
import os
import shutil
import pandas as pd

FIELD_KEYWORDS = {
    "date": ["date", "timestamp", "time"],
    "open": ["open"],
    "high": ["high"],
    "low": ["low"],
    "close": ["close", "adj close", "close_adj"],
    "volume": ["volume", "vol"]
}


def find_col(cols, keywords):
    cols_l = [c.lower() for c in cols]
    for kw in keywords:
        for i, c in enumerate(cols_l):
            if kw in c:
                return cols[i]
    return None


def try_read_csv(path):
    # try utf-8 then fallback to cp1252 (windows)
    for enc in ("utf-8", "cp1252"):
        try:
            df = pd.read_csv(path, dtype=str, encoding=enc)
            return df
        except Exception as e:
            last_exc = e
    raise last_exc


def normalize_file(path, out_path, backup=True):
    print(f"Reading {path} ...")
    try:
        df = try_read_csv(path)
    except Exception as e:
        print("ERROR reading", path, "-", e)
        return

    df = df.fillna("")
    df.columns = [c.strip() for c in df.columns]

    cols = df.columns.tolist()

    date_col = find_col(cols, FIELD_KEYWORDS["date"])
    open_col = find_col(cols, FIELD_KEYWORDS["open"])
    high_col = find_col(cols, FIELD_KEYWORDS["high"])
    low_col = find_col(cols, FIELD_KEYWORDS["low"])
    close_col = find_col(cols, FIELD_KEYWORDS["close"])
    vol_col = find_col(cols, FIELD_KEYWORDS["volume"])

    out = pd.DataFrame()

    # timestamp -> normalized 'timestamp' column (YYYY-MM-DD)
    if date_col:
        ts = pd.to_datetime(df[date_col], errors="coerce", utc=False)
        out["timestamp"] = ts.dt.strftime("%Y-%m-%d")
    else:
        out["timestamp"] = ""

    def clean(col):
        if not col:
            return pd.Series([""] * len(df))
        return df[col].astype(str).str.replace(",", "").str.strip()

    out["open"] = clean(open_col)
    out["high"] = clean(high_col)
    out["low"] = clean(low_col)
    out["close"] = clean(close_col)
    out["volume"] = clean(vol_col)

    # backup original if writing back to same path
    if backup and os.path.abspath(path) == os.path.abspath(out_path):
        bak = path + ".bak"
        if not os.path.exists(bak):
            shutil.copy2(path, bak)
            print("Backup:", bak)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    out.to_csv(out_path, index=False)
    print("[OK] wrote", out_path, "\n")


def main():
    p = argparse.ArgumentParser(prog="normalize_tickers.py")
    p.add_argument("--in-dir", default="data/tickers", help="input directory with ticker csvs")
    p.add_argument("--pattern", default="*.JK.csv", help="glob pattern for ticker files")
    p.add_argument("--out-dir", default=None, help="if set, write normalized files to this dir")
    p.add_argument("--backup", action="store_true", help="backup original files with .bak")
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.in_dir, args.pattern)))
    if not files:
        print("No files found. Checked:", os.path.join(args.in_dir, args.pattern))
        return

    for f in files:
        if args.out_dir:
            os.makedirs(args.out_dir, exist_ok=True)
            out = os.path.join(args.out_dir, os.path.basename(f))
        else:
            out = f
        normalize_file(f, out, backup=args.backup)


if __name__ == "__main__":
    main()
