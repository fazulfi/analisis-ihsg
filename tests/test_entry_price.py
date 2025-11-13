import pandas as pd
from indicators.entry_price import get_entry_price_for_signal

def test_entry_close():
    df = pd.DataFrame([
        {"date":"2025-11-01","open":100,"high":110,"low":90,"close":105},
        {"date":"2025-11-02","open":106,"high":112,"low":100,"close":110},
    ])
    price, note = get_entry_price_for_signal(df, 0, source="close")
    assert price == 105
    assert note is None

def test_entry_next_open_ok():
    df = pd.DataFrame([
        {"date":"2025-11-01","open":100,"high":110,"low":90,"close":105},
        {"date":"2025-11-02","open":106,"high":112,"low":100,"close":110},
    ])
    price, note = get_entry_price_for_signal(df, 0, source="next_open")
    assert price == 106
    assert note is None

def test_entry_next_open_no_next():
    df = pd.DataFrame([
        {"date":"2025-11-01","open":100,"high":110,"low":90,"close":105},
    ])
    price, note = get_entry_price_for_signal(df, 0, source="next_open")
    assert price is None
    assert note == "cannot_use_next_open"

def test_missing_columns():
    df = pd.DataFrame([{"date":"2025-11-01","close":100}])
    p, n = get_entry_price_for_signal(df, 0, source="next_open")
    assert p is None and n == "missing_open_column"
    p2, n2 = get_entry_price_for_signal(df, 0, source="close")
    assert p2 == 100 and n2 is None
