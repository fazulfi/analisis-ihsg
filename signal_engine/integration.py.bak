# signal_engine/integration.py
from typing import List, Dict, Any, Optional
import pandas as pd

from indicators.entry_price import get_entry_price_for_signal

def attach_atr_and_entry_to_signals(
    df: pd.DataFrame,
    signals: List[Dict[str, Any]],
    cfg: Dict[str, Any] = None,
    entry_price_source: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Attach atr_value and entry_price to each signal dict.

    Args:
      df: DataFrame with at least columns ['atr','open','close'].
      signals: list of dicts, each must have at least 'index' (int) and 'signal_type'.
               e.g. {"index": 20, "signal_type": "BUY", "date": "2025-11-xx"}
      cfg: optional config dict to pick entry_price_source if entry_price_source param omitted.
      entry_price_source: "close" or "next_open" (overrides cfg if provided).

    Returns:
      new_signals: list of dicts with added keys:
        - 'entry_price' (float or None)
        - 'atr_value' (float or None)
        - 'note' (str or None)  e.g. "insufficient_data_for_atr" or "cannot_use_next_open"
    """
    if cfg is None:
        cfg = {}
    if entry_price_source is None:
        entry_price_source = cfg.get("entry_price_source", "close")

    # normalize df colnames
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    out_signals = []
    for s in signals:
        sig = s.copy()
        idx = sig.get("index", None)
        if idx is None:
            sig.setdefault("note", "missing_index")
            out_signals.append(sig)
            continue

        # default values
        sig["entry_price"] = None
        sig["atr_value"] = None
        sig.setdefault("note", None)

        # check index bounds
        if idx < 0 or idx >= len(df):
            sig["note"] = "signal_index_out_of_range"
            out_signals.append(sig)
            continue

        # get atr_value
        if "atr" not in df.columns:
            sig["note"] = "atr_not_computed"
            out_signals.append(sig)
            continue

        atr_val = pd.to_numeric(df.loc[df.index[idx], "atr"], errors="coerce")
        if pd.isna(atr_val):
            sig["note"] = "insufficient_data_for_atr"
            sig["atr_value"] = None
            out_signals.append(sig)
            continue

        sig["atr_value"] = float(atr_val)

        # get entry_price using helper (handles next_open or close and returns notes)
        entry_price, entry_note = get_entry_price_for_signal(df, idx, source=entry_price_source)
        sig["entry_price"] = entry_price
        if entry_note is not None:
            sig["note"] = entry_note

        out_signals.append(sig)

    return out_signals
