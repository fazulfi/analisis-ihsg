# ingestor/storage.py
"""
Storage helper untuk menyimpan data historis (CSV + SQLite).
Fokus file ini: append_to_csv(rows, path="data/historical.csv").
"""

from pathlib import Path
import csv
from typing import List, Dict

FIELDNAMES = ["symbol", "timestamp", "open", "high", "low", "close", "volume", "source"]

def append_to_csv(rows: List[Dict], path: str = "data/historical.csv") -> None:
    """
    Append list of dict rows to CSV. Buat header kalau belum ada.
    rows: list of dict, setiap dict setidaknya punya keys di FIELDNAMES.
    path: lokasi file CSV relatif ke root project.
    """
    if not isinstance(rows, list):
        raise ValueError("rows harus berupa list of dict")

    csv_path = Path(path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = csv_path.exists()

    # buka file dalam mode append, newline='' supaya csv writer bekerja benar di Windows
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            # pastikan semua field ada (jika tidak ada, isi dengan empty string)
            row = {k: r.get(k, "") for k in FIELDNAMES}
            writer.writerow(row)
