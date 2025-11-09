# ingestor/test_save_csv.py
from datetime import datetime
from ingestor.storage import append_to_csv

def main():
    now = datetime.utcnow().isoformat()
    sample_rows = [
        {"symbol": "AALI.JK", "timestamp": now, "open": 1000, "high": 1010, "low": 995, "close": 1005, "volume": 120000, "source": "mock"},
        {"symbol": "BBCA.JK", "timestamp": now, "open": 8200, "high": 8250, "low": 8150, "close": 8220, "volume": 50000, "source": "mock"}
    ]
    append_to_csv(sample_rows)
    print("Selesai append_to_csv, cek file data/historical.csv")

if __name__ == "__main__":
    main()
