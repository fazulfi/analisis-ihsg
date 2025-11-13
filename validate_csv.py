#!/usr/bin/env python3
"""
validate_csv.py
Usage:
  python validate_csv.py DATA/<TICKER>.csv [--config config.yaml]

Exit codes:
  0 - OK
  2 - Usage / file not found / load config error
  3 - Missing required columns or bad types
  4 - Insufficient data (not enough rows for ATR)
  5 - Found NA in OHLC or date parse error
"""
import sys, os, json
from pathlib import Path

try:
    import yaml
except Exception:
    yaml = None

try:
    import pandas as pd
except Exception:
    pd = None

REQUIRED_COLUMNS = ["date", "open", "high", "low", "close", "volume"]

def load_config(path):
    if not os.path.exists(path):
        raise RuntimeError(f"Config file not found: {path}")
    ext = os.path.splitext(path)[1].lower()
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    if ext in (".yaml", ".yml"):
        if yaml is None:
            raise RuntimeError("PyYAML not installed. pip install pyyaml")
        return yaml.safe_load(text)
    elif ext == ".json":
        return json.loads(text)
    else:
        raise RuntimeError("Unsupported config format. Use .yaml/.json")

def print_err(*args):
    print(*args, file=sys.stderr)

def validate_csv(path, cfg):
    if pd is None:
        raise RuntimeError("pandas not installed. pip install pandas")

    if not os.path.exists(path):
        print_err(f"ERROR: CSV file not found: {path}")
        return 2

    # read csv
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print_err("ERROR reading CSV:", e)
        return 2

    # check required columns
    cols_lower = [c.lower() for c in df.columns]
    missing = [c for c in REQUIRED_COLUMNS if c not in cols_lower]
    # map to actual columns (case-insensitive)
    col_map = {c.lower(): c for c in df.columns}
    if missing:
        print_err("ERROR: Missing required columns:", ", ".join(missing))
        print_err("Found columns:", ", ".join(df.columns))
        return 3

    # normalize df columns to lower-case names for consistent access
    df.columns = [c.lower() for c in df.columns]

    # parse date
    try:
        df['date'] = pd.to_datetime(df['date'], errors='raise')
    except Exception as e:
        print_err("ERROR: Date parse failed:", e)
        return 5

    # check NA in OHLC
    ohlc = ['open', 'high', 'low', 'close']
    na_mask = df[ohlc].isnull().any(axis=1)
    if na_mask.any():
        idxs = df[na_mask].index.tolist()[:5]
        print_err("ERROR: Found NA in OHLC at rows (0-indexed):", idxs)
        return 5

    # check numeric types for OHLC and volume
    for c in ohlc + ['volume']:
        try:
            df[c] = pd.to_numeric(df[c], errors='raise')
        except Exception as e:
            print_err(f"ERROR: Column {c} must be numeric:", e)
            return 3

    # check length vs atr_period
    atr_period = cfg.get("atr_period", 14)
    needed = int(atr_period) + 1
    if len(df) < needed:
        print("STATUS: insufficient_data")
        print(f" - rows_found: {len(df)}, required: {needed} (atr_period + 1)")
        return 4

    # all good
    print("STATUS: OK")
    print(f" - rows: {len(df)}")
    print(f" - atr_period required rows: {needed}")
    return 0

def main():
    if len(sys.argv) < 2:
        print("Usage: python validate_csv.py DATA/<TICKER>.csv [--config config.yaml]")
        sys.exit(2)
    csv_path = sys.argv[1]
    cfg_path = "config.yaml"
    if len(sys.argv) >= 3 and sys.argv[2] in ("--config",):
        if len(sys.argv) < 4:
            print("Usage: python validate_csv.py <csv> --config <config.yaml>")
            sys.exit(2)
        cfg_path = sys.argv[3]

    try:
        cfg = load_config(cfg_path)
    except Exception as e:
        print_err("ERROR loading config:", e)
        sys.exit(2)

    rc = validate_csv(csv_path, cfg)
    sys.exit(rc)

if __name__ == "__main__":
    main()
