"""
signal_engine/postprocess.py

Enforce rule: only one open signal at a time per ticker.
Given attached signals (list of dicts with at least 'index','signal_type','entry_price','atr_value')
and the dataframe df (must contain 'high','low' columns), filter signals so that
a second signal cannot start before the previous one is closed (TP or SL hit).

Returns:
  filtered_signals: list of signals (a subset of original order)
  skipped: list of signals that were skipped (with note added)
"""
from typing import List, Dict, Tuple, Optional
import math

def _safe_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def enforce_single_open_signal(attached_signals: List[Dict], df, cfg: Optional[dict]=None) -> Tuple[List[Dict], List[Dict]]:
    """
    Params:
      attached_signals: list of dicts, each must have 'index' (int) or 'date', and may have entry_price, atr_value, note, signal_type
      df: pandas DataFrame with numeric columns 'high' and 'low' (lowercase) and indexed by integer positions aligning to signal 'index'
      cfg: optional dict with 'sl_multiplier' and 'tp_multiplier' to compute SL/TP if not already computed

    Behavior:
      - iterate signals in ascending index order
      - keep first signal, compute its SL/TP (requires atr_value & entry_price)
      - scan forward from (index+1) to find the first bar where SL or TP is hit
        - for BUY: TP hit if high >= tp ; SL hit if low <= sl
        - for SELL: TP hit if low <= tp ; SL hit if high >= sl
      - set next_allowed_index = found_index (if found) else INF (block all later signals)
      - skip any signals with index <= next_allowed_index, marking them with note 'skipped_open_trade'
    """
    import numpy as np

    if cfg is None:
        cfg = {}

    sl_mult = float(cfg.get("sl_multiplier", 1.5))
    tp_mult = float(cfg.get("tp_multiplier", 3.0))

    # normalize df columns to lowercase names
    colmap = {c.lower(): c for c in df.columns}
    if 'high' not in colmap or 'low' not in colmap:
        raise ValueError("DataFrame must contain 'high' and 'low' columns")

    highs = df[colmap['high']].values
    lows  = df[colmap['low']].values
    n = len(df)

    # sort signals by index (signals with no index go last and will be skipped)
    sigs_sorted = sorted(attached_signals, key=lambda s: (s.get('index') is None, s.get('index') if s.get('index') is not None else 10**12))

    filtered = []
    skipped = []
    next_allowed_index = -1  # any signal with index <= this is skipped

    INF = 10**12

    for s in sigs_sorted:
        idx = s.get('index')
        # if no index, we cannot place it reliably; skip with note
        if idx is None:
            s_note = s.get('note', '')
            s['note'] = (s_note + ';skipped_no_index').lstrip(';')
            skipped.append(s)
            continue

        # if this signal occurs before allowed index -> skip
        if idx <= next_allowed_index:
            s_note = s.get('note', '')
            s['note'] = (s_note + ';skipped_open_trade').lstrip(';')
            skipped.append(s)
            continue

        # accept this signal
        filtered.append(s)

        # compute entry, atr
        entry = _safe_float(s.get('entry_price'))
        atrv  = _safe_float(s.get('atr_value'))

        # if atr or entry missing -> cannot determine SL/TP; block subsequent signals forever (safe behavior)
        if entry is None or atrv is None or (math.isnan(entry) if entry is not None else False) or (math.isnan(atrv) if atrv is not None else False):
            # mark as having insufficient data - but still keep this signal; block further ones
            s_note = s.get('note', '')
            s['note'] = (s_note + ';insufficient_data_for_atr_or_entry_blocking').lstrip(';')
            next_allowed_index = INF
            continue

        # compute raw SL/TP (before rounding)
        sigtype = (s.get('signal_type') or 'BUY').upper()
        if sigtype == "BUY":
            sl_raw = entry - (atrv * sl_mult)
            tp_raw = entry + (atrv * tp_mult)
        else:
            # SELL
            sl_raw = entry + (atrv * sl_mult)
            tp_raw = entry - (atrv * tp_mult)

        # scan forward bars from idx+1 to find first hit
        found_close_index = None
        for j in range(int(idx)+1, n):
            if sigtype == "BUY":
                # check TP first (favour TP detection) then SL
                if highs[j] >= tp_raw:
                    found_close_index = j
                    break
                if lows[j] <= sl_raw:
                    found_close_index = j
                    break
            else:
                # SELL
                if lows[j] <= tp_raw:
                    found_close_index = j
                    break
                if highs[j] >= sl_raw:
                    found_close_index = j
                    break

        if found_close_index is None:
            # not closed within dataset -> block all subsequent signals (safer)
            next_allowed_index = INF
            s_note = s.get('note', '')
            s['note'] = (s_note + ';no_close_in_history_blocking_future').lstrip(';')
        else:
            # allow signals after the closing bar
            next_allowed_index = found_close_index

    return filtered, skipped
