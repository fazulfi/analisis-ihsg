#!/usr/bin/env python3
"""
calc_atr.py
Hitung ATR (Wilder) periode N dari kolom 'tr' pada CSV.
Jika kolom 'tr' tidak ada, script akan mencoba menghitung TR sederhana
menggunakan high, low, close (memanggil internal compute_tr).

Usage:
  python calc_atr.py <input.csv> <output.csv> [--config config.yaml]

Behavior:
 - ATR_first = mean(TR[0:N]) and placed at index N-1
 - ATR_t = ((ATR_{t-1} * (N-1)) + TR_t) / N for t >= N
 - For indices < N-1: atr = NaN
"""
import sys, os
from pathlib import Path

try:
    import pandas as pd
except Exception:
    pd = None

def compute_tr_if_missing(df):
    # compute TR using definition if 'tr' not present
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    for c in ("high","low","close"):
        if c not in df.columns:
            raise ValueError("Missing required column for TR computation: high/low/close")
    df['prev_close'] = df['close'].shift(1)
    cand1 = df['high'] - df['low']
    cand2 = (df['high'] - df['prev_close']).abs()
    cand3 = (df['low'] - df['prev_close']).abs()
    df['tr'] = pd.concat([cand1, cand2, cand3], axis=1).max(axis=1)
    df = df.drop(columns=['prev_close'])
    return df

def compute_atr_wilder(tr_series, n):
    # tr_series: pandas Series indexed same as df
    # returns pandas Series 'atr' aligned to same index
    tr = tr_series.astype(float)
    atr = pd.Series([float('nan')] * len(tr), index=tr.index)

    if len(tr) < n:
        # not enough data to compute first ATR
        return atr

    # ATR_first at index n-1: mean of tr[0:n]
    first_idx = tr.index[n-1]
    first_val = tr.iloc[0:n].mean()
    atr.iloc[n-1] = first_val

    # subsequent
    for i in range(n, len(tr)):
        prev_atr = atr.iloc[i-1]
        tr_i = tr.iloc[i]
        # Wilder smoothing
        atr_val = ((prev_atr * (n - 1)) + tr_i) / n
        atr.iloc[i] = atr_val

    return atr

def load_config(cfg_path):
    # minimal loader for atr_period from yaml or json
    import json
    ext = os.path.splitext(cfg_path)[1].lower()
    text = open(cfg_path, "r", encoding="utf-8").read()
    if ext in (".yaml", ".yml"):
        try:
            import yaml
        except Exception:
            raise RuntimeError("PyYAML not installed (pip install pyyaml)")
        return yaml.safe_load(text)
    else:
        return json.loads(text)

def main():
    if len(sys.argv) < 3:
        print("Usage: python calc_atr.py <input.csv> <output.csv> [--config config.yaml]")
        sys.exit(2)

    inpath = Path(sys.argv[1])
    outpath = Path(sys.argv[2])
    cfg_path = "config.yaml"
    if "--config" in sys.argv:
        i = sys.argv.index("--config")
        if i+1 < len(sys.argv):
            cfg_path = sys.argv[i+1]
        else:
            print("Missing config path after --config")
            sys.exit(2)

    if not inpath.exists():
        print("ERROR: input file not found:", inpath)
        sys.exit(2)
    if not Path(cfg_path).exists():
        print("ERROR: config file not found:", cfg_path)
        sys.exit(2)
    if pd is None:
        print("ERROR: pandas not installed. pip install pandas")
        sys.exit(3)

    cfg = load_config(cfg_path)
    n = int(cfg.get("atr_period", 14))

    try:
        df = pd.read_csv(inpath)
    except Exception as e:
        print("ERROR reading CSV:", e)
        sys.exit(3)

    # normalize column names
    df.columns = [c.lower() for c in df.columns]

    # ensure tr exists or compute it
    if "tr" not in df.columns:
        try:
            df = compute_tr_if_missing(df)
        except Exception as e:
            print("ERROR computing TR:", e)
            sys.exit(3)
    else:
        # ensure numeric
        df['tr'] = pd.to_numeric(df['tr'], errors='raise')

    try:
        atr_series = compute_atr_wilder(df['tr'], n)
    except Exception as e:
        print("ERROR computing ATR:", e)
        sys.exit(3)

    df['atr'] = atr_series

    try:
        outpath.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(outpath, index=False)
    except Exception as e:
        print("ERROR writing output CSV:", e)
        sys.exit(3)

    print("OK: ATR computed and saved to", outpath)
    sys.exit(0)

if __name__ == "__main__":
    main()
