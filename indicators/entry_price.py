# indicators/entry_price.py
from typing import Tuple, Optional
import pandas as pd

def get_entry_price_for_signal(df: pd.DataFrame, signal_idx: int, source: str = "close") -> Tuple[Optional[float], Optional[str]]:
    """
    Returns (entry_price, note)
    - df: dataframe with columns (must include 'open' and 'close' as needed). Column names are case-insensitive.
    - signal_idx: integer index (0-based) of the bar where the signal occurred.
    - source: "close" or "next_open"
    Notes:
      - If source=="close": returns df.loc[signal_idx, 'close'].
      - If source=="next_open": returns df.loc[signal_idx+1, 'open'] if exists, otherwise (None, 'cannot_use_next_open').
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame")

    # normalize column names
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    if source not in ("close", "next_open"):
        raise ValueError("source must be 'close' or 'next_open'")

    # bounds check
    if signal_idx < 0 or signal_idx >= len(df):
        return None, "signal_index_out_of_range"

    if source == "close":
        if 'close' not in df.columns:
            return None, "missing_close_column"
        val = pd.to_numeric(df.loc[df.index[signal_idx], 'close'], errors='coerce')
        if pd.isna(val):
            return None, "close_is_nan"
        return float(val), None

    # next_open
    if 'open' not in df.columns:
        return None, "missing_open_column"
    next_pos = signal_idx + 1
    if next_pos >= len(df):
        return None, "cannot_use_next_open"
    val = pd.to_numeric(df.loc[df.index[next_pos], 'open'], errors='coerce')
    if pd.isna(val):
        return None, "next_open_is_nan"
    return float(val), None
