#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
from indicators.atr import compute_tr, compute_atr_wilder

def load_config(cfg_path="config.yaml"):
    import os, json
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
        print("Usage: python scripts/calc_atr.py <input.csv> <output.csv> [--config config.yaml]")
        sys.exit(2)
    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])
    cfg_path = "config.yaml"
    if "--config" in sys.argv:
        i = sys.argv.index("--config")
        if i+1 < len(sys.argv):
            cfg_path = sys.argv[i+1]
        else:
            print("Missing config after --config"); sys.exit(2)
    cfg = load_config(cfg_path)
    n = int(cfg.get("atr_period", 14))

    if not inp.exists():
        print("Input not found:", inp); sys.exit(2)
    df = pd.read_csv(inp)
    if 'tr' not in [c.lower() for c in df.columns]:
        df = compute_tr(df)
    else:
        df.columns = [c.lower() for c in df.columns]
    df['atr'] = compute_atr_wilder(df['tr'], n)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print("OK: ATR computed ->", out)

if __name__ == "__main__":
    main()
