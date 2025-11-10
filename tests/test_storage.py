# tests/test_storage.py
import sqlite3
import csv
from pathlib import Path
import pandas as pd
import pytest

from ingestor import storage


def sample_rows():
    # dua baris unik + satu baris duplicate (sama symbol+timestamp)
    rows = [
        {
            "symbol": "AAA.JK",
            "timestamp": "2025-11-09T00:00:00+00:00",
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 104.0,
            "volume": 1000,
            "source": "test",
        },
        {
            "symbol": "BBB.JK",
            "timestamp": "2025-11-09T00:01:00+00:00",
            "open": 200.0,
            "high": 210.0,
            "low": 195.0,
            "close": 205.0,
            "volume": 2000,
            "source": "test",
        },
        # duplicate key for AAA.JK -> later upsert should replace/keep unique
        {
            "symbol": "AAA.JK",
            "timestamp": "2025-11-09T00:00:00+00:00",
            "open": 101.0,
            "high": 106.0,
            "low": 100.0,
            "close": 105.0,
            "volume": 1100,
            "source": "test2",
        },
    ]
    return rows


def test_append_to_csv_creates_file_and_writes(tmp_path):
    rows = sample_rows()[:2]  # gunakan dua baris unik
    csv_path = tmp_path / "data" / "historical.csv"
    # pastikan folder belum ada
    assert not csv_path.exists()
    storage.append_to_csv(rows, path=str(csv_path))
    assert csv_path.exists()
    # cek header & jumlah baris (header + 2 data)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = list(csv.reader(f))
    assert reader[0] == storage.FIELDNAMES
    assert len(reader) == 1 + len(rows)  # header + rows


def test_save_to_sqlite_writes_rows_and_upsert(tmp_path):
    rows = sample_rows()
    db_path = tmp_path / "db" / "historical.db"
    # save (first time)
    storage.save_to_sqlite(rows, dbpath=str(db_path))
    # check DB exists and row count equals unique keys (2 unique keys: AAA + BBB)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM historical;")
    count = cur.fetchone()[0]
    assert count == 2  # upsert removed duplicate
    # check that AAA.JK row has values from the last duplicate (upsert behavior)
    cur.execute("SELECT open, close, volume, source FROM historical WHERE symbol = ? AND timestamp = ?",
                ("AAA.JK", "2025-11-09T00:00:00+00:00"))
    r = cur.fetchone()
    assert r is not None
    open_val, close_val, vol_val, src = r
    assert open_val == pytest.approx(101.0)
    assert close_val == pytest.approx(105.0)
    assert vol_val == 1100
    assert src == "test2"
    conn.close()


def test_idempotent_upsert_does_not_duplicate_and_updates(tmp_path):
    rows = sample_rows()
    db_path = tmp_path / "db" / "historical.db"
    # first save
    storage.save_to_sqlite(rows, dbpath=str(db_path))
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM historical;")
    before = cur.fetchone()[0]
    # save again exactly same rows (should not increase count)
    storage.save_to_sqlite(rows, dbpath=str(db_path))
    cur.execute("SELECT COUNT(*) FROM historical;")
    after = cur.fetchone()[0]
    assert before == after
    # now modify one row (change close) and save -> count same but value updated
    modified = list(rows)
    modified[1] = dict(modified[1])  # change BBB.JK close
    modified[1]["close"] = 9999.0
    storage.save_to_sqlite(modified, dbpath=str(db_path))
    cur.execute("SELECT close FROM historical WHERE symbol=? AND timestamp=?",
                ("BBB.JK", "2025-11-09T00:01:00+00:00"))
    new_close = cur.fetchone()[0]
    assert new_close == pytest.approx(9999.0)
    conn.close()
