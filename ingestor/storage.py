# tambahkan di atas file: import os
import os
import sqlite3
from pathlib import Path
from typing import List, Dict, Optional

# ... pastikan FIELDNAMES dan append_to_csv masih ada di file

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

# Gunakan ON CONFLICT DO UPDATE (SQLite 3.24+), fallback ke INSERT OR REPLACE jika versi lama.
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

# Fallback string (older sqlite) — gunakan jika ON CONFLICT tidak didukung:
SQL_INSERT_REPLACE = """
INSERT OR REPLACE INTO historical
(symbol,timestamp,open,high,low,close,volume,source)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

def _choose_upsert_statement(conn: sqlite3.Connection):
    # cek apakah SQLite di lingkungan mendukung "ON CONFLICT ... DO UPDATE"
    cur = conn.cursor()
    try:
        cur.execute("SELECT sqlite_version();")
        ver = cur.fetchone()[0]
        # versi 3.24.0 (2018-06-04) memperkenalkan UPSERT syntax
        major, minor, patch = (int(x) for x in ver.split(".")[:3])
        if (major, minor, patch) >= (3, 24, 0):
            return SQL_INSERT_UPSERT
    except Exception:
        pass
    return SQL_INSERT_REPLACE

def save_to_sqlite(rows: List[Dict], dbpath: str = "db/historical.db", pragmas: Optional[Dict] = None) -> None:
    """
    Save rows to sqlite DB safely:
    - apply PRAGMA (WAL)
    - create table if not exists
    - write inside a single transaction (atomic)
    - use upsert to avoid duplicates
    """
    if not isinstance(rows, list):
        raise ValueError("rows harus berupa list of dict")

    db_file = Path(dbpath)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    # connect
    conn = sqlite3.connect(str(db_file), timeout=30, isolation_level=None)  # isolation_level=None -> manual transaction control
    try:
        # Set pragmas (default WAL for concurrency safety)
        if pragmas is None:
            pragmas = {"journal_mode": "WAL", "synchronous": "NORMAL"}
        for k, v in pragmas.items():
            # pragma value bisa string atau number
            conn.execute(f"PRAGMA {k}={v};")

        # ensure table exists
        conn.execute(SQL_CREATE)

        upsert_sql = _choose_upsert_statement(conn)

        cur = conn.cursor()
        # Begin transaction explicitly for atomicity
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
