# AUTOPATCHED_BY_apply_pipeline_patches: added robust imports and timestamp default
#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
try:
    from indicators.atr import compute_tr
except Exception:
    # fallback when running as script (adjust sys.path to include repo root)
    import sys, os
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    try:
        from indicators.atr import compute_tr
    except Exception:
        # last resort: try top-level import without package prefix
        modname = 'indicators.atr'.split('.')[-1]
        from importlib import import_module
        im = import_module(modname)
        for name in ['compute_tr']:
            globals()[name] = getattr(im, name)


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
