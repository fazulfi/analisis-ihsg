# indicators/sltp.py
from typing import Tuple, Optional
import math

def _round_to_tick(price: float, tick: float, mode: str) -> float:
    """
    Round a price to nearest tick according to mode:
    - mode == 'floor': round down to nearest multiple of tick
    - mode == 'ceil' : round up to nearest multiple of tick
    Returns float rounded.
    """
    if tick is None or tick == 0:
        return float(price)
    if tick < 0:
        raise ValueError("tick must be non-negative")
    mult = price / tick
    if mode == 'floor':
        return float(math.floor(mult) * tick)
    elif mode == 'ceil':
        return float(math.ceil(mult) * tick)
    else:
        raise ValueError("mode must be 'floor' or 'ceil'")

def compute_sltp_for_signal(
    entry_price: float,
    atr_value: float,
    sl_multiplier: float = 1.5,
    tp_multiplier: float = 3.0,
    tick_size: Optional[float] = None,
    signal_type: str = "BUY",
    min_positive_tick: Optional[float] = None
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Compute SL and TP given entry and atr.

    Returns (sl_price, tp_price, note)
    - If cannot compute, returns appropriate note string and None for prices.
    """
    # validate
    if entry_price is None:
        return None, None, "missing_entry_price"
    if atr_value is None:
        return None, None, "missing_atr"
    if signal_type not in ("BUY", "SELL"):
        return None, None, "invalid_signal_type"

    # handle atr zero
    if atr_value == 0:
        sl = float(entry_price)
        tp = float(entry_price)
        note = "atr_zero_warning"
        # still round if tick_size provided
        if tick_size:
            if signal_type == "BUY":
                sl = _round_to_tick(sl, tick_size, "floor")
                tp = _round_to_tick(tp, tick_size, "ceil")
            else:  # SELL
                sl = _round_to_tick(sl, tick_size, "ceil")
                tp = _round_to_tick(tp, tick_size, "floor")
        # ensure positive
        if sl <= 0:
            note = (note + "; sl_non_positive")
            if min_positive_tick:
                sl = float(min_positive_tick)
        return sl, tp, note

    # compute raw
    if signal_type == "BUY":
        raw_sl = float(entry_price) - float(atr_value) * float(sl_multiplier)
        raw_tp = float(entry_price) + float(atr_value) * float(tp_multiplier)
    else:  # SELL
        raw_sl = float(entry_price) + float(atr_value) * float(sl_multiplier)
        raw_tp = float(entry_price) - float(atr_value) * float(tp_multiplier)

    # rounding
    if tick_size and tick_size > 0:
        if signal_type == "BUY":
            sl = _round_to_tick(raw_sl, tick_size, "floor")
            tp = _round_to_tick(raw_tp, tick_size, "ceil")
        else:  # SELL
            sl = _round_to_tick(raw_sl, tick_size, "ceil")
            tp = _round_to_tick(raw_tp, tick_size, "floor")
    else:
        sl = float(raw_sl)
        tp = float(raw_tp)

    note = None
    # handle sl_non_positive
    if sl <= 0:
        note = "sl_non_positive"
        # cap if min_positive_tick provided
        if min_positive_tick and min_positive_tick > 0:
            sl = float(min_positive_tick)

    return sl, tp, note
