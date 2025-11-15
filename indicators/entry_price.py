# indicators/entry_price.py
from typing import Tuple, Optional
import pandas as pd

def resolve_entry_price_for_signal(
    signal_row: dict,
    prices: pd.DataFrame,
    timestamp_col: str = "timestamp",
    entry_price_source: str = "close",
) -> Tuple[Optional[float], str, str]:
    """
    Resolve entry price for a single signal row.

    Params:
      - signal_row: mapping-like row for the signal (must contain timestamp or index)
      - prices: dataframe of price bars indexed/column including timestamp_col, and columns: open, close, high, low, volume
      - timestamp_col: column name in `prices` that contains the bar timestamp (or index is used)
      - entry_price_source: "close" or "next_open"

    Returns:
      (entry_price_or_None, entry_price_source_used, note)
      - entry_price_or_None: float or None if cannot resolve
      - entry_price_source_used: "close" or "next_open" or "missing"
      - note: "" or explanatory message (e.g. "cannot_use_next_open")
    """
    # Normalize prices timestamps if needed
    df = prices.copy()
    if timestamp_col in df.columns:
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
        df = df.sort_values(by=timestamp_col).reset_index(drop=True)
    else:
        # assume datetime index; convert to column for uniform handling
        try:
            df = df.reset_index().rename(columns={df.index.name: timestamp_col})
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
        except Exception:
            # fallback: leave as-is
            pass

    # Extract signal timestamp: accept either 'timestamp' or 'date' or index
    sig_ts = None
    for k in ("timestamp", "date", "time"):
        if k in signal_row and signal_row.get(k) not in (None, ""):
            try:
                sig_ts = pd.to_datetime(signal_row.get(k))
            except Exception:
                sig_ts = None
            break

    # If no timestamp -> try index field
    if sig_ts is None and ("index" in signal_row):
        try:
            sig_ts = pd.to_datetime(signal_row.get("index"))
        except Exception:
            sig_ts = None

    # if still none, return missing
    if sig_ts is None:
        return None, "missing", "missing_timestamp"

    # find row matching timestamp (exact). There might be multiple formats, so compare by date/time equality.
    # Use merge-on-equality after normalizing to datetime
    matches = df[df[timestamp_col] == sig_ts]
    if len(matches) == 0:
        # try tolerant matching: maybe string date vs datetime with time 00:00:00
        try:
            sig_date = sig_ts.normalize()
            matches = df[df[timestamp_col].dt.normalize() == sig_date]
        except Exception:
            matches = df[df[timestamp_col] == sig_ts]

    if len(matches) == 0:
        return None, "missing", "timestamp_not_found_in_prices"

    # pick first matching row (should be one)
    row_idx = matches.index[0]
    if entry_price_source == "close":
        # use close on the same bar
        val = matches.iloc[0].get("close", None)
        try:
            if pd.isna(val) or val == "":
                return None, "close", "close_missing"
            return float(val), "close", ""
        except Exception:
            return None, "close", "close_not_numeric"

    elif entry_price_source == "next_open":
        # prefer open of next bar
        next_idx = row_idx + 1
        if next_idx >= len(df):
            return None, "missing", "cannot_use_next_open"
        val = df.iloc[next_idx].get("open", None)
        try:
            if pd.isna(val) or val == "":
                return None, "next_open", "next_open_missing"
            return float(val), "next_open", ""
        except Exception:
            return None, "next_open", "next_open_not_numeric"

    else:
        return None, "missing", f"unknown_entry_price_source:{entry_price_source}"
