"""ingestor/storage.py
Storage helper: append_to_csv + save_to_sqlite.
Schema: symbol TEXT, timestamp TEXT (ISO8601), open/high/low/close REAL, volume INTEGER, source TEXT
Primary key: (symbol, timestamp)
"""

from pathlib import Path
import csv
from typing import List, Dict, Optional
import sqlite3
import os

FIELDNAMES = ["symbol", "timestamp", "open", "high", "low", "close", "volume", "source"]

def append_to_csv(rows: List[Dict], path: str = "data/historical.csv") -> None:
    """Append list of dict rows to CSV. Create header if not exists."""
    if not isinstance(rows, list):
        raise ValueError("rows harus berupa list of dict")
    csv_path = Path(path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in FIELDNAMES})

# ---------- SQLite part ----------
SQL_CREATE = """
CREATE TABLE IF NOT EXISTS historical (
  symbol TEXT NOT NULL,
  timestamp TEXT NOT NULL,
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  volume INTEGER,
  source TEXT,
  PRIMARY KEY (symbol, timestamp)
);
"""

SQL_INSERT_UPSERT = """
INSERT INTO historical (symbol,timestamp,open,high,low,close,volume,source)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol,timestamp) DO UPDATE SET
  open=excluded.open,
  high=excluded.high,
  low=excluded.low,
  close=excluded.close,
  volume=excluded.volume,
  source=excluded.source;
"""

SQL_INSERT_REPLACE = """
INSERT OR REPLACE INTO historical
(symbol,timestamp,open,high,low,close,volume,source)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

def _choose_upsert_statement(conn: sqlite3.Connection):
    cur = conn.cursor()
    try:
        cur.execute("SELECT sqlite_version();")
        ver = cur.fetchone()[0]
        major, minor, patch = (int(x) for x in ver.split(".")[:3])
        if (major, minor, patch) >= (3, 24, 0):
            return SQL_INSERT_UPSERT
    except Exception:
        pass
    return SQL_INSERT_REPLACE

def save_to_sqlite(rows: List[Dict], dbpath: str = "db/historical.db", pragmas: Optional[Dict] = None) -> None:
    """Save rows to sqlite DB safely with transaction & upsert."""
    if not isinstance(rows, list):
        raise ValueError("rows harus berupa list of dict")

    db_file = Path(dbpath)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_file), timeout=30, isolation_level=None)
    try:
        if pragmas is None:
            pragmas = {"journal_mode": "WAL", "synchronous": "NORMAL"}
        for k, v in pragmas.items():
            conn.execute(f"PRAGMA {k}={v};")
        conn.execute(SQL_CREATE)
        upsert_sql = _choose_upsert_statement(conn)
        cur = conn.cursor()
        cur.execute("BEGIN;")
        try:
            for r in rows:
                cur.execute(upsert_sql, (
                    r.get("symbol"),
                    r.get("timestamp"),
                    r.get("open"),
                    r.get("high"),
                    r.get("low"),
                    r.get("close"),
                    r.get("volume"),
                    r.get("source"),
                ))
            cur.execute("COMMIT;")
        except Exception:
            cur.execute("ROLLBACK;")
            raise
    finally:
        conn.close()

# tambahkan ke ingestor/storage.py (di bagian bawah)
def ensure_db_indexes(dbpath: str = "db/historical.db"):
    import sqlite3
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    # ensure table exists as safety (won't change if exists)
    cur.execute(SQL_CREATE)
    # create indexes if not exists
    cur.execute("CREATE INDEX IF NOT EXISTS idx_historical_timestamp ON historical(timestamp);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_historical_symbol ON historical(symbol);")
    conn.commit()
    conn.close()
