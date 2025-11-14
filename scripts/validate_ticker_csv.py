#!/usr/bin/env python3
"""
Validate one ticker CSV (DATA/<TICKER>.csv) for Step 1.

Checks:
 - required columns exist (date/timestamp, open, high, low, close, volume)
 - timestamp column parseable to datetime
 - OHLC not all-NA for rows (no NA in o/h/l/c)
 - number of bars >= atr_period + 1 (if less -> insufficient_data)
 - prints JSON-like summary and exits with:
     0 -> OK
     2 -> INSUFFICIENT_DATA
     3 -> ERROR
"""

import argparse
import sys
import os
import pandas as pd

REQUIRED_COLS = {"open", "high", "low", "close", "volume"}

def parse_args():
    p = argparse.ArgumentParser(description="Validate ticker CSV file for pipeline (Step 1).")
    p.add_argument("csv", help="Path to ticker CSV file (DATA/<TICKER>.csv or normalized/..)")
    p.add_argument("--atr-period", "-a", type=int, default=14, help="ATR period to check minimum bars (default: 14)")
    p.add_argument("--timestamp-col", "-t", default="timestamp", help="Name of timestamp column (default: timestamp)")
    p.add_argument("--min-ideal", type=int, default=30, help="Ideal minimum bars for comfortable operation (default: 30)")
    return p.parse_args()

def load_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"data file not found: {path}")
    # read as strings then clean
    df = pd.read_csv(path, dtype=str).fillna("")
    # normalize column names to lower and strip spaces
    cols = [c.strip() for c in df.columns.tolist()]
    df.columns = cols
    return df

def ensure_timestamp(df, timestamp_col):
    # try case-insensitive match
    colmap = {c.lower(): c for c in df.columns}
    if timestamp_col.lower() in colmap:
        actual = colmap[timestamp_col.lower()]
    else:
        # try common names
        for cand in ("date", "time", "datetime", "date_time", "trading_date"):
            if cand in colmap:
                actual = colmap[cand]
                break
        else:
            raise KeyError(f"Missing required time column: '{timestamp_col}' (or date/datetime).")
    # parse to datetime
    ser = pd.to_datetime(df[actual], errors="coerce")
    if ser.isnull().all():
        raise ValueError(f"Timestamp column '{actual}' could not be parsed to datetimes (all NaT).")
    # attach parsed column as integer seconds (unix)
    df["_parsed_ts"] = ser.astype("datetime64[ns]")
    # drop rows without timestamp (pipeline expects bars)
    df = df[df["_parsed_ts"].notna()].copy()
    df = df.sort_values(by="_parsed_ts").reset_index(drop=True)
    return df, actual

def check_required_cols(df):
    lower_cols = {c.lower() for c in df.columns}
    missing = [c for c in REQUIRED_COLS if c not in lower_cols]
    if missing:
        raise KeyError(f"Missing required OHLC/volume columns: {missing}")

def check_ohlc_non_na(df):
    # ensure numeric (coerce) and no complete NA on ohlc for remaining rows
    # map to lowercase columns
    colmap = {c.lower(): c for c in df.columns}
    o = colmap.get("open")
    h = colmap.get("high")
    l = colmap.get("low")
    c = colmap.get("close")
    df[o] = pd.to_numeric(df[o], errors="coerce")
    df[h] = pd.to_numeric(df[h], errors="coerce")
    df[l] = pd.to_numeric(df[l], errors="coerce")
    df[c] = pd.to_numeric(df[c], errors="coerce")
    # drop rows where all OHLC are NA
    before = len(df)
    df = df[~(df[o].isna() & df[h].isna() & df[l].isna() & df[c].isna())].copy()
    dropped = before - len(df)
    return df, dropped

def make_summary(ok, reason, details):
    out = {
        "status": "OK" if ok else reason,
        "details": details
    }
    return out

def main():
    args = parse_args()
    path = args.csv
    atr = int(args.atr_period)
    min_ideal = int(args.min_ideal)

    try:
        df = load_csv(path)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print(f"ERROR reading file: {e}", file=sys.stderr)
        sys.exit(3)

    # checks
    try:
        df_parsed, ts_col = ensure_timestamp(df, args.timestamp_col)
    except KeyError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(3)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(3)

    try:
        check_required_cols(df_parsed)
    except KeyError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(3)

    # check ohlc numeric / missing
    df_clean, dropped = check_ohlc_non_na(df_parsed)

    total_bars = len(df_clean)
    details = {
        "file": path,
        "timestamp_col": ts_col,
        "total_rows_after_parse": int(total_bars),
        "dropped_all_na_ohlc_rows": int(dropped),
        "atr_period": atr,
        "min_required": atr + 1,
        "min_ideal": min_ideal
    }

    if total_bars < (atr + 1):
        summary = make_summary(False, "INSUFFICIENT_DATA", details)
        print("SUMMARY")
        print(summary)
        sys.exit(2)

    # warn if less than ideal
    if total_bars < min_ideal:
        details["warning"] = f"barely_enough (bars >= {atr+1} but < ideal {min_ideal})"

    # ok
    summary = make_summary(True, "OK", details)
    print("SUMMARY")
    print(summary)
    sys.exit(0)

if __name__ == "__main__":
    main()
