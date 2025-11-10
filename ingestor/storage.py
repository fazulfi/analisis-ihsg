# ingestor/storage.py
"""
Storage helper: append_to_csv + save_to_sqlite with minimal logging.
Logs:
 - file logs/save.log records success/errors from storage operations.
 - logger name: ingestor.storage
"""

from pathlib import Path
import csv
from typing import List, Dict, Optional
import sqlite3
import os
import logging

# ---------- logging setup for this module ----------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
SAVE_LOG = LOG_DIR / "save.log"

logger = logging.getLogger("ingestor.storage")
# add a FileHandler only once (avoid duplicate handlers)
if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(SAVE_LOG) for h in logger.handlers):
    fh = logging.FileHandler(SAVE_LOG, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
# do not change global level here; let application configure root logger
# but ensure logger has a sensible level:
if logger.level == logging.NOTSET:
    logger.setLevel(logging.INFO)

# ---------- storage code ----------
FIELDNAMES = ["symbol", "timestamp", "open", "high", "low", "close", "volume", "source"]

def append_to_csv(rows: List[Dict], path: str = "data/historical.csv") -> None:
    """
    Append list of dict rows to CSV. Create header if not exists.
    Logs success/error to logs/save.log.
    """
    try:
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

        logger.info("append_to_csv OK (%d rows) -> %s", len(rows), csv_path)
    except Exception as e:
        logger.exception("append_to_csv ERROR: %s", e)
        # re-raise so caller can decide how to proceed
        raise

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
    """
    Save rows to sqlite DB safely:
    - apply PRAGMA (WAL)
    - create table if not exists
    - write inside a single transaction (atomic)
    - use upsert to avoid duplicates
    Logs success/errors to logs/save.log.
    """
    try:
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

        logger.info("save_to_sqlite OK (%d rows) -> %s", len(rows), db_file)
    except Exception as e:
        logger.exception("save_to_sqlite ERROR: %s", e)
        # re-raise so caller (fetcher) can continue gracefully if it chooses to catch
        raise
