#!/usr/bin/env python3

# ---- BEGIN: robust import for indicators (added by assistant) ----
try:
    # normal package-style import (preferred)
    from indicators.sltp import compute_sltp_for_signal
except ModuleNotFoundError:
    # fallback when script is run directly (e.g. `python scripts/...`) or via subprocess
    import sys, os
    # add project root (one level up from scripts/) to sys.path
    project_root = os.path.dirname(os.path.dirname(__file__))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from indicators.sltp import compute_sltp_for_signal
# ---- END: robust import for indicators ----


"""
scripts/calc_sltp_rounding.py
Single-step: compute SL/TP and apply tick rounding.

Usage:
  python scripts/calc_sltp_rounding.py <signals_input.csv> <signals_output.csv> [--config config.yaml]

Input CSV must contain columns (case-insensitive):
  - signal_type, entry_price, atr_value
Optional existing: index, date, note

Output CSV will contain original columns +:
  - sl_price, tp_price, sltp_note, sl_price_rounded, tp_price_rounded
"""
import sys
from pathlib import Path
import pandas as pd

# reuse our indicator functions
from indicators.sltp import compute_sltp_for_signal
from indicators.rounding import enforce_tick_rounding_on_signals

def load_config(cfg_path="config.yaml"):
    import json
    ext = Path(cfg_path).suffix.lower()
    text = open(cfg_path, "r", encoding="utf-8").read()
    if ext in (".yaml", ".yml"):
        try:
            import yaml
        except Exception:
            raise RuntimeError("Install pyyaml (pip install pyyaml)")
        return yaml.safe_load(text)
    return json.loads(text)

def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/calc_sltp_rounding.py <signals_input.csv> <signals_output.csv> [--config config.yaml]")
        sys.exit(2)
    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])
    cfg_path = "config.yaml"
    if "--config" in sys.argv:
        i = sys.argv.index("--config")
        cfg_path = sys.argv[i+1]

    cfg = load_config(cfg_path)
    sl_mult = float(cfg.get("sl_multiplier", 1.5))
    tp_mult = float(cfg.get("tp_multiplier", 3.0))
    tick_size = cfg.get("tick_size", None)
    min_tick = cfg.get("tick_size", None)

    if not inp.exists():
        print("Input signals CSV not found:", inp); sys.exit(2)
    df = pd.read_csv(inp)
    # normalize columns
    df.columns = [c.lower() for c in df.columns]

    # check required columns
    for c in ("signal_type","entry_price","atr_value"):
        if c not in df.columns:
            print("ERROR: input CSV must contain column:", c); sys.exit(3)

    sl_list = []
    tp_list = []
    note_list = []

    for i, row in df.iterrows():
        prev_note = row.get("note") if "note" in df.columns else None
        # preserve pre-existing note (skip calc)
        if prev_note and str(prev_note).strip() != "nan" and prev_note != "":
            sl_list.append(None)
            tp_list.append(None)
            note_list.append(prev_note)
            continue

        signal_type = (row.get('signal_type') or "").upper()
        entry = row['entry_price'] if not pd.isna(row['entry_price']) else None
        atr = row['atr_value'] if not pd.isna(row['atr_value']) else None

        sl, tp, note = compute_sltp_for_signal(entry, atr, sl_mult, tp_mult, tick_size, signal_type, min_positive_tick=min_tick)
        sl_list.append(sl)
        tp_list.append(tp)
        # if there is already a note from compute function, record it
        note_list.append(note)

    df['sl_price'] = sl_list
    df['tp_price'] = tp_list
    df['sltp_note'] = note_list

    # apply rounding enforcement using existing utility (it will fallback if tick invalid)
    rows = df.to_dict(orient="records")
    new_rows, warnings = enforce_tick_rounding_on_signals(rows, tick_size, default_behavior_if_invalid="no_round")
    df_out = pd.DataFrame(new_rows)

    out.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out, index=False)
    print("OK: SL/TP computed + rounding ->", out)
    if warnings:
        print("Warnings:")
        for w in warnings:
            print(" -", w)

if __name__ == "__main__":
    main()
