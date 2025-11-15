import subprocess
import sys
from pathlib import Path
import pandas as pd

SCRIPT = "scripts/validate_ticker_csv.py"

def write_csv(path, rows):
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)

def run(cmd):
    res = subprocess.run([sys.executable] + cmd, capture_output=True, text=True)
    return res

def test_ok(tmp_path):
    p = tmp_path / "ok.csv"
    rows = [
        {"timestamp":"2023-01-01","open":1,"high":2,"low":1,"close":1.5,"volume":100},
    ]
    # duplicate rows to satisfy ATR default 14
    rows = rows * 20
    write_csv(p, rows)
    res = run([SCRIPT, str(p)])
    assert res.returncode == 0
    assert "OK" in res.stdout

def test_insufficient(tmp_path):
    p = tmp_path / "short.csv"
    rows = [
        {"timestamp":"2023-01-01","open":1,"high":2,"low":1,"close":1.5,"volume":100},
    ] * 5
    write_csv(p, rows)
    res = run([SCRIPT, str(p), "--atr-period", "14"])
    assert res.returncode == 2

def test_missing_col(tmp_path):
    p = tmp_path / "miss.csv"
    rows = [
        {"timestamp":"2023-01-01","open":1,"high":2,"low":1,"volume":100},
    ] * 20
    write_csv(p, rows)
    res = run([SCRIPT, str(p)])
    assert res.returncode == 3
    assert "Missing required OHLC/volume columns" in (res.stderr + res.stdout)
