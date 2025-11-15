#!/usr/bin/env python3
"""
Fill missing entry_price and atr_value in signals CSVs from normalized price files.

Usage:
  python scripts/fill_signals_from_price.py \
     --signals-dir OUTPUT --signals-pattern "*_signals.csv" \
     --normalized-dir normalized --atr-period 14 --timestamp-col timestamp

It will backup original signals file to <file>.bak before overwriting.
"""
import argparse
import glob
import os
import shutil
import math
import pandas as pd
import numpy as np

def compute_atr(df, period=14):
    # expects df with columns: high, low, close (numeric)
    hi = df['high'].astype(float)
    lo = df['low'].astype(float)
    cl = df['close'].astype(float)
    tr1 = hi - lo
    tr2 = (hi - cl.shift(1)).abs()
    tr3 = (lo - cl.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    # Wilder's moving average (exponential with alpha=1/period) or simple SMA?
    # We'll use classic Wilder (EMA-like) using .ewm with adjust=False alpha=1/period
    atr = tr.ewm(alpha=1.0/period, adjust=False).mean()
    return atr

def load_price_for_ticker(normalized_dir, ticker):
    # tries multiple filename patterns
    candidates = [
        os.path.join(normalized_dir, f"{ticker}.csv"),
        os.path.join(normalized_dir, f"{ticker}.JK.csv"),
        os.path.join(normalized_dir, f"{ticker}.with_date.csv"),
        os.path.join(normalized_dir, f"{ticker}.normalized.csv"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    # fallback: attempt to find file that contains ticker in name
    pattern = os.path.join(normalized_dir, f"*{ticker}*.csv")
    found = glob.glob(pattern)
    if found:
        return found[0]
    return None

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--signals-dir", default="OUTPUT", help="dir containing signals files")
    p.add_argument("--signals-pattern", default="*_signals.csv", help="glob pattern for signals")
    p.add_argument("--normalized-dir", default="normalized", help="dir containing normalized price csvs")
    p.add_argument("--atr-period", type=int, default=14, help="ATR period")
    p.add_argument("--timestamp-col", default="timestamp", help="timestamp column name in normalized files")
    p.add_argument("--dry-run", action="store_true", help="don't overwrite files, just print summary")
    return p.parse_args()

def to_float_safe(x):
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)): return None
        s = str(x).strip()
        if s == "": return None
        return float(s)
    except:
        return None

def fill_file(signals_fpath, normalized_dir, atr_period, timestamp_col, dry_run=False):
    base = os.path.basename(signals_fpath)
    # heuristic to find ticker from filename: <TICKER>_signals.csv or OUTPUT/TICKER_signals.csv
    ticker = base.replace("_signals.csv","").replace("OUTPUT\\","").replace("OUTPUT/","")
    price_path = load_price_for_ticker(normalized_dir, ticker)
    if price_path is None:
        print(f"[SKIP] {signals_fpath}: cannot find normalized price for ticker '{ticker}' in {normalized_dir}")
        return {"file": signals_fpath, "skipped": True}

    df_sig = pd.read_csv(signals_fpath, dtype=str).fillna("")
    # normalize columns to lower for lookup but keep original names
    cols_map = {c.lower(): c for c in df_sig.columns}
    has_index = 'index' in cols_map
    has_date = 'date' in cols_map

    # load price
    df_price = pd.read_csv(price_path, dtype=str).fillna("")
    # normalize normalized price columns to expected names (case-insensitive)
    price_cols = {c.lower(): c for c in df_price.columns}
    required = ['open','high','low','close']
    for r in required:
        if r not in price_cols:
            # try common variations: Price, Close_BBRI.JK etc => try to detect 'close' by presence
            # if no direct mapping, fail
            pass
    # Map real column names
    try:
        open_col = price_cols.get('open', None) or price_cols.get('price', None)
        high_col = price_cols.get('high', None)
        low_col = price_cols.get('low', None)
        close_col = price_cols.get('close', None)
        if not (open_col and high_col and low_col and close_col):
            # try sensible fallback by column position if header is weird
            # common case: first col is timestamp, then close, high, low, open, volume (older format)
            # attempt to detect names by heuristics
            lc = list(df_price.columns)
            lc_lower = [c.lower() for c in lc]
            if 'timestamp' in lc_lower:
                idx_ts = lc_lower.index('timestamp')
                # choose next columns heuristically
            # if we can't find, raise
            raise ValueError("price file missing O/H/L/C columns")
    except Exception as e:
        print(f"[ERROR] cannot find OHLC in {price_path}: {e}")
        return {"file": signals_fpath, "skipped": True}

    # rename price columns to standard names
    df_price_ren = df_price.rename(columns={open_col:'open', high_col:'high', low_col:'low', close_col:'close'})
    # ensure timestamp parsed
    if timestamp_col in df_price_ren.columns:
        df_price_ren[timestamp_col] = pd.to_datetime(df_price_ren[timestamp_col], errors='coerce')
    else:
        # try common alternatives
        for cand in ('date','Date','DATE'):
            if cand in df_price_ren.columns:
                df_price_ren[timestamp_col] = pd.to_datetime(df_price_ren[cand], errors='coerce')
                break
    # compute ATR
    try:
        df_price_ren['open'] = df_price_ren['open'].astype(float)
        df_price_ren['high'] = df_price_ren['high'].astype(float)
        df_price_ren['low'] = df_price_ren['low'].astype(float)
        df_price_ren['close'] = df_price_ren['close'].astype(float)
    except Exception:
        # coerce non-numeric to NaN then drop?
        for c in ('open','high','low','close'):
            df_price_ren[c] = pd.to_numeric(df_price_ren[c], errors='coerce')
    atr_series = compute_atr(df_price_ren, period=atr_period)

    changed = 0
    for i, row in df_sig.iterrows():
        # check existing
        cur_entry = row.get(cols_map.get('entry_price','entry_price'), "").strip() if 'entry_price' in cols_map else ""
        cur_atr = row.get(cols_map.get('atr_value','atr_value'), "").strip() if 'atr_value' in cols_map else ""
        need_entry = (cur_entry == "")
        need_atr = (cur_atr == "")

        if not (need_entry or need_atr):
            continue

        chosen_close = None
        chosen_atr = None

        if has_index:
            idx_colname = cols_map['index']
            idx_val = row.get(idx_colname, "").strip()
            try:
                idx_int = int(idx_val)
            except:
                idx_int = None
            if idx_int is not None and 0 <= idx_int < len(df_price_ren):
                chosen_close = df_price_ren.iloc[idx_int]['close']
                chosen_atr = atr_series.iloc[idx_int]
        if chosen_close is None and has_date:
            date_colname = cols_map['date']
            dstr = row.get(date_colname, "").strip()
            if dstr:
                try:
                    dt = pd.to_datetime(dstr, errors='coerce')
                    if pd.isna(dt):
                        dt = None
                except:
                    dt = None
                if dt is not None:
                    # exact match
                    mask = df_price_ren[timestamp_col]==dt if timestamp_col in df_price_ren.columns else pd.Series(False, index=df_price_ren.index)
                    if mask.any():
                        ix = mask[mask].index[0]
                        chosen_close = df_price_ren.loc[ix,'close']
                        chosen_atr = atr_series.loc[ix]
                    else:
                        # take last bar before dt
                        if timestamp_col in df_price_ren.columns:
                            before = df_price_ren[df_price_ren[timestamp_col] <= dt]
                            if len(before):
                                ix = before.index[-1]
                                chosen_close = df_price_ren.loc[ix,'close']
                                chosen_atr = atr_series.loc[ix]

        # fallback: if still none, skip
        if chosen_close is None and chosen_atr is None:
            continue

        # write back into df_sig (use the actual column name if present, else create)
        ep_col = cols_map.get('entry_price', 'entry_price')
        atr_col = cols_map.get('atr_value', 'atr_value')
        if need_entry and chosen_close is not None:
            df_sig.at[i, ep_col] = "{:.6g}".format(float(chosen_close))
            changed += 1
        if need_atr and (chosen_atr is not None and not pd.isna(chosen_atr)):
            df_sig.at[i, atr_col] = "{:.6g}".format(float(chosen_atr))
            changed += 1

    if changed > 0 and not dry_run:
        bak = signals_fpath + ".bak"
        if not os.path.exists(bak):
            shutil.copyfile(signals_fpath, bak)
        df_sig.to_csv(signals_fpath, index=False)
        print(f"[FIXED] {signals_fpath}: wrote {changed} fields (backup -> {bak})")
    else:
        print(f"[OK/NOCHANGE] {signals_fpath}: changed={changed}")
    return {"file": signals_fpath, "changed": changed}

def main():
    args = parse_args()
    pattern = os.path.join(args.signals_dir, args.signals_pattern)
    files = sorted(glob.glob(pattern))
    if not files:
        print("No signal files found for pattern:", pattern)
        return
    summary = {"processed":0, "changed":0, "skipped":0}
    for f in files:
        res = fill_file(f, args.normalized_dir, args.atr_period, args.timestamp_col, dry_run=args.dry_run)
        summary["processed"] += 1
        if res.get("skipped"):
            summary["skipped"] += 1
        else:
            summary["changed"] += res.get("changed",0)
    print("Done. processed:", summary["processed"], "changed fields:", summary["changed"], "skipped files:", summary["skipped"])

if __name__ == "__main__":
    main()
