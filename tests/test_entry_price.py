# tests/test_entry_price.py
import pandas as pd
from indicators.entry_price import resolve_entry_price_for_signal

def make_prices():
    rows = [
        {"timestamp":"2025-01-01", "open":100, "high":110, "low":90, "close":105, "volume":1000},
        {"timestamp":"2025-01-02", "open":106, "high":116, "low":96, "close":110, "volume":1000},
        {"timestamp":"2025-01-03", "open":111, "high":121, "low":101, "close":115, "volume":1000},
    ]
    return pd.DataFrame(rows)

def test_entry_price_from_close():
    prices = make_prices()
    sig = {"timestamp":"2025-01-02"}
    price, used, note = resolve_entry_price_for_signal(sig, prices, "timestamp", "close")
    assert price == 110.0
    assert used == "close"
    assert note == ""

def test_entry_price_next_open_available():
    prices = make_prices()
    sig = {"timestamp":"2025-01-01"}
    price, used, note = resolve_entry_price_for_signal(sig, prices, "timestamp", "next_open")
    # next open for 2025-01-01 is open on 2025-01-02
    assert price == 106.0
    assert used == "next_open"
    assert note == ""

def test_entry_price_next_open_missing():
    prices = make_prices()
    sig = {"timestamp":"2025-01-03"}
    price, used, note = resolve_entry_price_for_signal(sig, prices, "timestamp", "next_open")
    assert price is None
    assert used == "missing"
    assert note == "cannot_use_next_open"

def test_missing_timestamp_in_signal():
    prices = make_prices()
    sig = {"date":""}  # empty
    price, used, note = resolve_entry_price_for_signal(sig, prices, "timestamp", "close")
    assert price is None
    assert used == "missing"
    assert "timestamp" in note
