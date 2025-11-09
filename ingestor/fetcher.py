# ingestor/fetcher.py
"""
Fetcher simple untuk project analisis-ihsg.
Default: mode 'mock' (generate sample data).
Cara pakai:
  python -m ingestor.fetcher          # run default mock flow
  python -m ingestor.fetcher mock     # sama seperti default
  python -m ingestor.fetcher live     # placeholder untuk integrasi API nyata
"""

from datetime import datetime, timezone
import time
import random
import logging
from typing import List, Dict
from pathlib import Path

from ingestor.storage import append_to_csv, save_to_sqlite

# config sederhana
TICKERS_FILE = "tickers.txt"
DEFAULT_TICKERS = ["AALI.JK", "BBCA.JK", "TLKM.JK", "BBRI.JK"]
DEFAULT_SOURCE = "mock"

# setup logging
import logging, sys
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", filename="logs/fetcher.log")
logger = logging.getLogger("fetcher")
# juga log ke stdout
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
    """Generate satu baris data mock (OHLCV)."""
    # basis harga random
    base = random.uniform(500, 10000)
    o = round(base, 2)
    # variasi kecil
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


def fetch_data(mode: str = "mock", max_requests_per_run: int = 50) -> List[Dict]:
    """
    Ambil data untuk semua tickers.
    mode:
      - 'mock': generate sample data
      - 'live': placeholder (implementasi API nyata nanti)
    max_requests_per_run: batasi jumlah request / ticker agar aman
    """
    tickers = load_tickers()
    rows = []
    logger.info("fetch_data start. mode=%s tickers=%s", mode, tickers)

    if mode == "mock":
        for i, t in enumerate(tickers):
            if i >= max_requests_per_run:
                logger.info("Max requests reached (%d)", max_requests_per_run)
                break
            try:
                row = generate_mock_row(t)
                rows.append(row)
                # sedikit delay supaya timestamp beda, dan terasa real
                time.sleep(0.05)
            except Exception as e:
                logger.exception("Gagal generate mock for %s: %s", t, e)
    elif mode == "live":
        # Placeholder: implementasikan pemanggilan API nyata di sini.
        # Contoh: panggil yfinance, atau API broker/marketdata, lalu map ke schema.
        logger.warning("Mode 'live' belum diimplementasikan. Gunakan 'mock' dulu.")
    else:
        logger.error("Mode tidak dikenal: %s", mode)

    logger.info("fetch_data done. rows=%d", len(rows))
    return rows


def run_once(mode: str = "mock") -> None:
    """Run full pipeline: fetch -> save CSV & SQLite."""
    logger.info("Run once start. mode=%s", mode)
    rows = fetch_data(mode=mode)
    if not rows:
        logger.info("No rows fetched. exit.")
        return

    try:
        append_to_csv(rows)
        logger.info("append_to_csv OK (%d rows)", len(rows))
    except Exception as e:
        logger.exception("Gagal append_to_csv: %s", e)

    try:
        save_to_sqlite(rows)
        logger.info("save_to_sqlite OK (%d rows)", len(rows))
    except Exception as e:
        logger.exception("Gagal save_to_sqlite: %s", e)

    logger.info("Run once finished.")


def main():
    import sys
    mode = "mock"
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    run_once(mode=mode)


if __name__ == "__main__":
    main()
