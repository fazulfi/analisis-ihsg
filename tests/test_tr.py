import pandas as pd
import math

# import compute_tr sesuai modul project
from indicators.atr import compute_tr


def _to_series_tr(maybe):
    """Normalize return type from compute_tr to Pandas Series."""
    if isinstance(maybe, pd.Series):
        return maybe.astype(float).reset_index(drop=True)
    elif isinstance(maybe, pd.DataFrame):
        # try common TR column names
        for c in maybe.columns:
            if c.lower() in ("tr", "true_range", "atr_tr"):
                return maybe[c].astype(float).reset_index(drop=True)
        # fallback: first numeric column
        return pd.to_numeric(maybe.iloc[:, 0], errors="coerce").astype(float)
    else:
        return pd.Series(maybe, dtype=float)


def test_tr_digit_by_digit():
    """
    TR calculation basic test (digit by digit).

    Bar0: TR = high - low
    Bar1+: TR = max(high-low, |high-prev_close|, |low-prev_close|)
    """
    rows = [
        {"date": "2025-01-01", "open": 1000, "high": 1050, "low": 1000, "close": 1040, "volume": 1000},
        {"date": "2025-01-02", "open": 1020, "high": 1080, "low": 1040, "close": 1075, "volume": 1000},
        {"date": "2025-01-03", "open": 1075, "high": 1115, "low": 1075, "close": 1100, "volume": 1000},
        {"date": "2025-01-04", "open": 1100, "high": 1170, "low": 1100, "close": 1160, "volume": 1000},
    ]
    df = pd.DataFrame(rows)

    tr_raw = compute_tr(df)
    tr_series = _to_series_tr(tr_raw)

    # corrected expected values according to true-range formula
    expected = [50.0, 40.0, 40.0, 70.0]

    assert len(tr_series) == len(expected)
    for i, e in enumerate(expected):
        assert math.isclose(float(tr_series.iloc[i]), e, rel_tol=1e-9), (
            f"TR mismatch at index {i}: expected {e}, got {tr_series.iloc[i]}"
        )
