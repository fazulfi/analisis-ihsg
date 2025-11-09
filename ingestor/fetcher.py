# ingestor/fetcher.py
"""
Fetcher pipeline — integrasi storage CSV + SQLite.
Mode default: 'mock' untuk testing offline.
Run:
  python -m ingestor.fetcher
  python -m ingestor.fetcher mock
  python -m ingestor.fetcher live
"""

from datetime import datetime, timezone
import time
import random
import logging
import sys
from typing import List, Dict
from pathlib import Path

# import storage functions (integrasi)
from ingestor.storage import append_to_csv, save_to_sqlite, FIELDNAMES

# config
TICKERS_FILE = "tickers.txt"
DEFAULT_TICKERS = ["AALI.JK", "BBCA.JK", "TLKM.JK", "BBRI.JK", "BMRI.JK", "UNVR.JK"]
DEFAULT_SOURCE = "mock"
LOGFILE = "logs/fetcher.log"

# logging: file + stdout
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    filename=LOGFILE)
logger = logging.getLogger("fetcher")
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.INFO)
sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(sh)


def load_tickers(path: str = TICKERS_FILE) -> List[str]:
    p = Path(path)
    if p.exists():
        try:
            lines = [l.strip() for l in p.read_text(encoding="utf-8").splitlines()]
            ticks = [l for l in lines if l and not l.startswith("#")]
            if ticks:
                return ticks
        except Exception as e:
            logger.warning("Gagal baca tickers dari %s: %s", path, e)
    return DEFAULT_TICKERS


def generate_mock_row(symbol: str) -> Dict:
    base = random.uniform(500, 10000)
    o = round(base, 2)
    h = round(base * (1 + random.uniform(0, 0.02)), 2)
    l = round(base * (1 - random.uniform(0, 0.02)), 2)
    c = round(random.uniform(l, h), 2)
    vol = random.randint(1000, 500000)
    ts = datetime.now(timezone.utc).isoformat()
    return {
        "symbol": symbol,
        "timestamp": ts,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": vol,
        "source": DEFAULT_SOURCE,
    }


def fetch_data(mode: str = "mock", max_requests_per_run: int = 100) -> List[Dict]:
    tickers = load_tickers()
    rows: List[Dict] = []
    logger.info("fetch_data start. mode=%s tickers=%s", mode, tickers)

    if mode == "mock":
        for i, t in enumerate(tickers):
            if i >= max_requests_per_run:
                break
            try:
                rows.append(generate_mock_row(t))
                time.sleep(0.02)
            except Exception as e:
                logger.exception("Gagal generate mock for %s: %s", t, e)
    elif mode == "live":
        # placeholder: implement API call here
        logger.warning("Mode 'live' belum diimplementasikan. Gunakan 'mock' atau implementasikan live fetch.")
    else:
        logger.error("Mode tidak dikenal: %s", mode)

    logger.info("fetch_data done. rows=%d", len(rows))
    return rows


def persist_rows(rows: List[Dict], csv_path: str = "data/historical.csv", db_path: str = "db/historical.db") -> None:
    """Panggil append_to_csv dan save_to_sqlite dengan error handling."""
    if not rows:
        logger.info("persist_rows called with empty rows. Nothing to do.")
        return

    # ensure keys match FIELDNAMES
    for r in rows:
        for f in FIELDNAMES:
            if f not in r:
                r[f] = ""

    # append to csv (safe)
    try:
        append_to_csv(rows, path=csv_path)
        logger.info("append_to_csv OK (%d rows)", len(rows))
    except Exception as e:
        logger.exception("Gagal append_to_csv: %s", e)

    # save to sqlite (safe)
    try:
        save_to_sqlite(rows, dbpath=db_path)
        logger.info("save_to_sqlite OK (%d rows)", len(rows))
    except Exception as e:
        logger.exception("Gagal save_to_sqlite: %s", e)


def run_once(mode: str = "mock"):
    logger.info("Run once start. mode=%s", mode)
    rows = fetch_data(mode=mode)
    if not rows:
        logger.info("No rows fetched. Exiting run.")
        return
    persist_rows(rows)
    logger.info("Run once finished.")


def main():
    mode = "mock"
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    run_once(mode=mode)


if __name__ == "__main__":
    main()
