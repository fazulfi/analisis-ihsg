import os
import csv
import subprocess
import pandas as pd
import tempfile
import sys

def test_calc_sltp_rounding_writes_csv(tmp_path):
    inp = tmp_path / "inp.csv"
    out = tmp_path / "out.csv"
    inp.write_text("index,date,signal_type,entry_price,atr_value,note\n2,2025-11-03,BUY,1009.0,10.0,\n3,2025-11-04,BUY,1005.0,0.0,atr_zero_warning\n4,2025-11-05,BUY,2.0,10.0,\n")
    # use same python interpreter as pytest process
    cmd = [sys.executable, "scripts/calc_sltp_rounding.py", str(inp), str(out), "--config", "config.yaml"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"script failed: {res.stdout}\n{res.stderr}"
    assert out.exists()
    df = pd.read_csv(out)
    # expected columns (lower/upper tolerance)
    cols = [c.lower() for c in df.columns]
    for col in ['date','signal_type','entry_price','atr_value','sl_price','tp_price','sl_price_rounded','tp_price_rounded','sltp_note']:
        assert col in cols, f"missing column {col}"
    # check that atr_zero_warning present
    notes = df['sltp_note'].astype(str).tolist()
    assert any('atr_zero_warning' in n for n in notes)
