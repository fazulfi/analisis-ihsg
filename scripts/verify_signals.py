#!/usr/bin/env python3
"""
verify_signals.py

Usage:
  python scripts/verify_signals.py <signal_csv_or_glob>... <config.yaml> [--timestamp-col TIMESTAMP_COL]

Example:
  python scripts/verify_signals.py OUTPUT/*_signals.csv config.yaml --timestamp-col timestamp

Function:
  - Untuk setiap file signal CSV, baca rows yang punya entry_price dan atr_value.
  - Gunakan config.yaml untuk sl_multiplier, tp_multiplier, tick_size.
  - Hitung expected raw SL/TP (aritmetik) dan expected rounded (round to tick using floor/ceil).
  - Bandingkan dengan kolom yang ditemukan di CSV (sl_price, tp_price, sl_price_rounded, tp_price_rounded).
  - Laporkan summary dan list beberapa mismatch.
"""
import sys
import glob
import yaml
import pandas as pd
import math
import os

def tofloat(x):
    try:
        return float(x)
    except Exception:
        return None

def round_tick(price, tick, mode):
    # mode: 'floor' or 'ceil'
    if price is None or tick is None or tick == 0:
        return None
    q = price / tick
    if mode == "floor":
        return math.floor(q) * tick
    else:
        return math.ceil(q) * tick

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        c = yaml.safe_load(f)
    # defaults
    sl_mult = c.get("s", {}).get("sl_multiplier", c.get("sl_multiplier", 1.5))
    tp_mult = c.get("s", {}).get("tp_multiplier", c.get("tp_multiplier", 3.0))
    tick_size = c.get("tick_size", 1)
    return {"sl_multiplier": float(sl_mult), "tp_multiplier": float(tp_mult), "tick_size": float(tick_size)}

def normalize_cols(df):
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def find_val_cols(df):
    # possible column names
    mapping = {
        "entry_price": ["entry_price", "entry"],
        "atr_value": ["atr_value", "atr"],
        "sl_price": ["sl_price", "sl"],
        "tp_price": ["tp_price", "tp"],
        "sl_price_rounded": ["sl_price_rounded", "sl_rounded"],
        "tp_price_rounded": ["tp_price_rounded", "tp_rounded"],
        "notes": ["notes","note"]
    }
    cols = {}
    for k, candidates in mapping.items():
        for cand in candidates:
            if cand in df.columns:
                cols[k] = cand
                break
        else:
            cols[k] = None
    return cols

def process_file(path, cfg):
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception as e:
        return {"file": path, "error": f"read_error: {e}"}
    df = normalize_cols(df)
    cols = find_val_cols(df)

    if cols["entry_price"] is None or cols["atr_value"] is None:
        return {"file": path, "error": "missing_entry_or_atr_column"}

    results = {"file": path, "total": 0, "ok_raw": 0, "ok_rounded": 0, "insufficient": 0, "errors": [], "mismatches": []}

    sl_mult = cfg["sl_multiplier"]
    tp_mult = cfg["tp_multiplier"]
    tick = cfg["tick_size"]

    # iterate rows
    for i, row in df.iterrows():
        results["total"] += 1
        entry_raw = row.get(cols["entry_price"], "").strip()
        atr_raw = row.get(cols["atr_value"], "").strip()
        note = row.get(cols["notes"], "") if cols["notes"] else ""

        if entry_raw == "" or atr_raw == "":
            results["insufficient"] += 1
            continue

        entry = tofloat(entry_raw)
        atr = tofloat(atr_raw)
        if entry is None or atr is None:
            results["errors"].append({"row": i, "reason": "non_numeric_entry_or_atr"})
            continue

        sig = (row.get("signal_type") or row.get("signal")).strip().upper() if ("signal_type" in df.columns or "signal" in df.columns) else "BUY"
        if sig == "":
            sig = "BUY"

        # expected raw
        if sig == "BUY":
            exp_sl = entry - (atr * sl_mult)
            exp_tp = entry + (atr * tp_mult)
        else:
            # SELL
            exp_sl = entry + (atr * sl_mult)
            exp_tp = entry - (atr * tp_mult)

        # rounding expectation
        # For BUY: SL should be rounded up? Original used ceil for BUY tp and floor for sl maybe —
        # We follow rule used in verifier: BUY -> sl floor, tp ceil (so SL not worse than allowed)
        exp_sl_rounded = round_tick(exp_sl, tick, "floor")
        exp_tp_rounded = round_tick(exp_tp, tick, "ceil")

        # read found columns
        found_sl = tofloat(row.get(cols["sl_price"], "") if cols["sl_price"] else "")
        found_tp = tofloat(row.get(cols["tp_price"], "") if cols["tp_price"] else "")
        found_sl_r = tofloat(row.get(cols["sl_price_rounded"], "") if cols["sl_price_rounded"] else (row.get(cols["sl_price"], "") if cols["sl_price"] else ""))
        found_tp_r = tofloat(row.get(cols["tp_price_rounded"], "") if cols["tp_price_rounded"] else (row.get(cols["tp_price"], "") if cols["tp_price"] else ""))

        # check raw match (exact numeric equality)
        raw_ok = (found_sl is not None and abs(found_sl - exp_sl) == 0) and (found_tp is not None and abs(found_tp - exp_tp) == 0)
        rounded_ok = (found_sl_r is not None and abs(found_sl_r - exp_sl_rounded) == 0) and (found_tp_r is not None and abs(found_tp_r - exp_tp_rounded) == 0)

        # Count
        if raw_ok:
            results["ok_raw"] += 1
        if rounded_ok:
            results["ok_rounded"] += 1
        if not raw_ok and not rounded_ok:
            # record mismatch
            results["mismatches"].append({
                "row": i,
                "sig": sig,
                "entry": entry,
                "atr": atr,
                "expected_raw": (exp_sl, exp_tp),
                "found_raw": (found_sl, found_tp),
                "expected_rounded": (exp_sl_rounded, exp_tp_rounded),
                "found_rounded": (found_sl_r, found_tp_r),
                "note": note
            })

    return results

def summarize(all_results):
    total_rows = sum(r.get("total",0) for r in all_results if "total" in r)
    ok_raw = sum(r.get("ok_raw",0) for r in all_results if "total" in r)
    ok_rounded = sum(r.get("ok_rounded",0) for r in all_results if "total" in r)
    insufficient = sum(r.get("insufficient",0) for r in all_results if "total" in r)
    errors = []
    mismatches = []
    for r in all_results:
        if r.get("error"):
            errors.append({"file": r["file"], "error": r["error"]})
        else:
            mismatches.extend([{"file": r["file"], **m} for m in r.get("mismatches", [])])

    print("SUMMARY")
    print(" total_rows:", total_rows)
    print(" ok_raw:", ok_raw)
    print(" ok_rounded:", ok_rounded)
    print(" insufficient_flagged:", insufficient)
    print(" errors_count:", len(errors))
    print(" mismatches_count:", len(mismatches))
    if len(errors):
        print("\nerrors (first 10):")
        for e in errors[:10]:
            print(e)
    if len(mismatches):
        print("\nFound mismatches (first 10):")
        for m in mismatches[:10]:
            print(m)
    # acceptance metric: raw match ratio
    raw_rate = (ok_raw / total_rows) if total_rows>0 else 0.0
    print("\nACCEPTANCE:", "PASS" if raw_rate >= 0.90 else "FAIL", f"(raw_rate = {raw_rate:.2%})")
    return {"total_rows": total_rows, "ok_raw": ok_raw, "ok_rounded": ok_rounded, "insufficient": insufficient, "errors": errors, "mismatches": mismatches}

def main(argv):
    if len(argv) < 3:
        print("Usage: python scripts/verify_signals.py <signal_csv1> [<signal_csv2> ...] <config.yaml> [--timestamp-col TIMESTAMP_COL]")
        sys.exit(2)

    # parse args: last non-option is config.yaml
    args = argv[1:]
    timestamp_col = None
    if "--timestamp-col" in args:
        i = args.index("--timestamp-col")
        if i+1 < len(args):
            timestamp_col = args[i+1]
            # remove from args
            args.pop(i); args.pop(i)
        else:
            print("ERROR: --timestamp-col requires a value"); sys.exit(2)

    # last arg is config
    cfg_path = args[-1]
    files = []
    for p in args[:-1]:
        # expand glob
        files.extend(sorted(glob.glob(p)))
    if not files:
        print("No input files found for patterns:", args[:-1])
        sys.exit(1)
    cfg = load_config(cfg_path)

    all_results = []
    for f in files:
        r = process_file(f, cfg)
        all_results.append(r)

    summary = summarize(all_results)
    # exit non-zero if acceptance fail
    raw_rate = (summary["ok_raw"] / summary["total_rows"]) if summary["total_rows"]>0 else 0.0
    sys.exit(0 if raw_rate >= 0.90 else 1)

if __name__ == "__main__":
    main(sys.argv)
