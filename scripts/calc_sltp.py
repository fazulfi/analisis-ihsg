#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
from indicators.sltp import compute_sltp_for_signal

def load_config(cfg_path="config.yaml"):
    import json, os
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
        print("Usage: python scripts/calc_sltp.py <signals_input.csv> <signals_output.csv> [--config config.yaml]")
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
    min_tick = cfg.get("tick_size", None)  # default min positive cap = tick_size

    if not inp.exists():
        print("Input signals CSV not found:", inp); sys.exit(2)
    df = pd.read_csv(inp)
    df.columns = [c.lower() for c in df.columns]

    # ensure columns exist
    for c in ("signal_type","entry_price","atr_value"):
        if c not in df.columns:
            print("ERROR: input CSV must contain column:", c); sys.exit(3)

    sl_list = []
    tp_list = []
    note_list = []

    for i, row in df.iterrows():
        # if a pre-existing note (like insufficient_data_for_atr), preserve
        prev_note = row.get("note") if "note" in df.columns else None
        if prev_note and str(prev_note).strip() != "nan" and prev_note != "":
            sl_list.append(None)
            tp_list.append(None)
            note_list.append(prev_note)
            continue

        signal_type = row['signal_type']
        entry = row['entry_price'] if not pd.isna(row['entry_price']) else None
        atr = row['atr_value'] if not pd.isna(row['atr_value']) else None

        sl, tp, note = compute_sltp_for_signal(entry, atr, sl_mult, tp_mult, tick_size, signal_type, min_positive_tick=min_tick)
        sl_list.append(sl)
        tp_list.append(tp)
        # merge notes
        merged_note = note if note else None
        note_list.append(merged_note)

    df['sl_price'] = sl_list
    df['tp_price'] = tp_list
    df['sltp_note'] = note_list

    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print("OK: SL/TP computed ->", out)

if __name__ == "__main__":
    main()
