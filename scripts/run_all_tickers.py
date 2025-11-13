#!/usr/bin/env python3
"""
scripts/run_all_tickers.py

Scan data/tickers for files matching pattern (default '*.JK.csv'), run the single-ticker
pipeline for each, and produce a summary CSV at OUTPUT/pipeline_summary.csv.

Usage (from repo root, venv active):
  python scripts/run_all_tickers.py [--data-dir data/tickers] [--pattern '*.JK.csv'] [--signals-dir signals] [--config config.yaml] [--outdir OUTPUT] [--parallel N]

Examples:
  python scripts/run_all_tickers.py
  python scripts/run_all_tickers.py --parallel 4
  python scripts/run_all_tickers.py --pattern '*.csv' --signals-dir signals --config config_test_atr.yaml
"""
import argparse
from pathlib import Path
import subprocess
import time
import csv
import sys
import shlex

def run_ticker_pipeline(pipeline_py, ticker, data_file, signals_file, config_file, timestamp_col, outdir, append_mode=False):
    # Build command
    cmd = [
        sys.executable, str(pipeline_py),
        ticker,
        "--data", str(data_file),
        "--signals", str(signals_file),
        "--config", str(config_file),
        "--timestamp-col", timestamp_col
    ]
    if append_mode:
        cmd.append("--append")
    # return (returncode, stdout+stderr)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=900)  # 15 min timeout per ticker
        out = proc.stdout + proc.stderr
        return proc.returncode, out
    except Exception as e:
        return 99, str(e)

def find_signals_for_ticker(signals_dir, ticker):
    # prefer signals/<TICKER>_signals.csv, else None
    cand = Path(signals_dir) / f"{ticker}_signals.csv"
    if cand.exists():
        return cand
    # try signals file by ticker with different suffixes
    for ext in (".csv", ".signals.csv"):
        alt = Path(signals_dir) / f"{ticker}{ext}"
        if alt.exists():
            return alt
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data/tickers", help="directory containing ticker CSVs")
    parser.add_argument("--pattern", default="*.JK.csv", help="glob pattern to match ticker files")
    parser.add_argument("--signals-dir", default="signals", help="directory where per-ticker signals CSV are stored")
    parser.add_argument("--config", default="config.yaml", help="path to config file")
    parser.add_argument("--outdir", default="OUTPUT", help="output directory (used by pipeline as well)")
    parser.add_argument("--pipeline-script", default="scripts/run_pipeline.py", help="single-ticker pipeline script path")
    parser.add_argument("--timestamp-col", default="timestamp", help="timestamp column name in data files (case-insensitive)")
    parser.add_argument("--parallel", type=int, default=0, help=">0 to run N processes in parallel (multiprocessing). 0 = serial.")
    parser.add_argument("--pattern-is-regexp", action="store_true", help="treat pattern as regexp (not used by default)")
    parser.add_argument("--append", action="store_true", help="pass --append to pipeline script (appends instead of overwrite)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    pipeline_py = Path(args.pipeline_script)
    signals_dir = Path(args.signals_dir)
    outdir = Path(args.outdir)
    config_file = Path(args.config)
    timestamp_col = args.timestamp_col
    append_mode = args.append

    if not data_dir.exists():
        print("Data dir not found:", data_dir)
        return 2
    if not pipeline_py.exists():
        print("Pipeline script not found:", pipeline_py)
        return 3
    if not config_file.exists():
        print("Config not found:", config_file)
        return 4

    files = sorted(data_dir.glob(args.pattern))
    if not files:
        print("No files found matching pattern:", args.pattern, "in", data_dir)
        return 0

    summary_rows = []
    # header for summary CSV
    summary_path = outdir / "pipeline_summary.csv"
    outdir.mkdir(parents=True, exist_ok=True)

    # serial or naive parallel
    if args.parallel and args.parallel > 1:
        # simple multiprocessing pool
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel) as ex:
            futures = {}
            for f in files:
                ticker = f.stem.split('.')[0]  # BBRI.JK.csv -> 'BBRI'
                # better ticker extraction: remove suffix .JK if present
                if ticker.endswith(".JK"):
                    tname = ticker.split(".")[0]
                else:
                    tname = ticker
                sig_file = find_signals_for_ticker(signals_dir, tname) or (signals_dir / f"{tname}_signals.csv")
                futures[ex.submit(run_ticker_pipeline, pipeline_py, tname, f, sig_file if sig_file else Path(""), config_file, timestamp_col, outdir, append_mode)] = (tname, f, sig_file)
            for fut in concurrent.futures.as_completed(futures):
                tname, fpath, sig = futures[fut]
                try:
                    rc, out = fut.result()
                except Exception as e:
                    rc = 99
                    out = str(e)
                note = ""
                if rc == 0:
                    status = "ok"
                else:
                    status = f"error_{rc}"
                    note = out.splitlines()[-10:] if isinstance(out, str) else str(out)
                    note = "\\n".join(note)
                summary_rows.append({
                    "ticker": tname,
                    "input_file": str(fpath),
                    "signals_file": str(sig) if sig else "",
                    "status": status,
                    "notes": note,
                    "output_file": str(outdir / f"{tname}_signals.csv")
                })
    else:
        for f in files:
            t0 = time.time()
            # determine ticker: try 'BBRI.JK.csv' -> ticker 'BBRI'
            stem = f.stem  # 'BBRI.JK' or 'BBRI'
            # if pattern endswith .JK.csv, split on dot
            tname = stem.split('.')[0]
            sig_file = find_signals_for_ticker(signals_dir, tname) or (signals_dir / f"{tname}_signals.csv")
            if sig_file and not sig_file.exists():
                sig_file = None
            # run
            rc, out = run_ticker_pipeline(pipeline_py, tname, f, sig_file if sig_file else Path(""), config_file, timestamp_col, outdir, append_mode)
            took = time.time() - t0
            note = ""
            if rc == 0:
                status = "ok"
            else:
                status = f"error_{rc}"
                note = out.splitlines()[-20:] if isinstance(out, str) else str(out)
                note = "\\n".join(note)
            summary_rows.append({
                "ticker": tname,
                "input_file": str(f),
                "signals_file": str(sig_file) if sig_file else "",
                "status": status,
                "notes": note,
                "time_s": f"{took:.2f}",
                "output_file": str(outdir / f"{tname}_signals.csv")
            })
            print(f"[{tname}] -> {status} (time {took:.2f}s)")

    # write summary CSV
    with open(summary_path, "w", newline='', encoding='utf-8') as fh:
        fieldnames = ["ticker","input_file","signals_file","status","time_s","output_file","notes"]
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in summary_rows:
            # ensure all keys
            row = {k: r.get(k,"") for k in fieldnames}
            w.writerow(row)

    print("Done. Summary:", summary_path)
    return 0

if __name__ == "__main__":
    sys.exit(main())
