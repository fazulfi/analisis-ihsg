#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
from indicators.rounding import enforce_tick_rounding_on_signals

def load_config(cfg_path="config.yaml"):
    import json
    ext = Path(cfg_path).suffix.lower()
    text = open(cfg_path, "r", encoding="utf-8").read()
    if ext in (".yaml", ".yml"):
        try:
            import yaml
        except Exception:
            raise RuntimeError("Install pyyaml")
        return yaml.safe_load(text)
    return json.loads(text)

def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/enforce_rounding.py <signals_input.csv> <signals_output.csv> [--config config.yaml]")
        sys.exit(2)
    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])
    cfg_path = "config.yaml"
    if "--config" in sys.argv:
        i = sys.argv.index("--config")
        cfg_path = sys.argv[i+1]

    cfg = load_config(cfg_path)
    tick = cfg.get("tick_size", None)

    if not inp.exists():
        print("Input not found:", inp); sys.exit(2)
    df = pd.read_csv(inp)
    df.columns = [c.lower() for c in df.columns]

    # Prepare list of dicts (rows)
    rows = df.to_dict(orient="records")
    new_rows, warnings = enforce_tick_rounding_on_signals(rows, tick, default_behavior_if_invalid="no_round")
    df_out = pd.DataFrame(new_rows)
    out.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out, index=False)
    print("OK: rounding enforced ->", out)
    if warnings:
        print("Warnings:")
        for w in warnings:
            print(" -", w)

if __name__ == "__main__":
    main()
