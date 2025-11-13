#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

def normalize(path_in, path_out=None):
    p = Path(path_in)
    if not p.exists():
        print("File not found:", p); return 2
    df = pd.read_csv(p)
    cols_lower = {c.lower(): c for c in df.columns}
    if 'date' not in cols_lower:
        if 'timestamp' in cols_lower:
            col = cols_lower['timestamp']
            ts = df[col]
            if pd.api.types.is_integer_dtype(ts) or pd.api.types.is_float_dtype(ts):
                sample = int(ts.dropna().iloc[0])
                unit = 's' if sample < 1e11 else 'ms'
                if unit == 'ms':
                    df['date'] = pd.to_datetime(df[col], unit='ms').dt.strftime('%Y-%m-%d')
                else:
                    df['date'] = pd.to_datetime(df[col], unit='s').dt.strftime('%Y-%m-%d')
            else:
                df['date'] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                if df['date'].isnull().any():
                    print("Warning: some timestamps could not be parsed")
        else:
            raise SystemExit("ERROR: cannot find 'date' or 'timestamp' column")
    out = path_out or str(p.parent / (p.stem + ".with_date" + p.suffix))
    df.to_csv(out, index=False)
    print("Wrote normalized file:", out)
    return 0

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: scripts/normalize_data.py <input.csv> [output.csv]")
        sys.exit(2)
    sys.exit(normalize(sys.argv[1], sys.argv[2] if len(sys.argv)>2 else None))
