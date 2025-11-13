#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
from indicators.atr import compute_tr

def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/calc_tr.py <input.csv> <output.csv>")
        sys.exit(2)
    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])
    if not inp.exists():
        print("Input not found:", inp); sys.exit(2)
    df = pd.read_csv(inp)
    df_out = compute_tr(df)
    out.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out, index=False)
    print("OK: TR added ->", out)

if __name__ == "__main__":
    main()
