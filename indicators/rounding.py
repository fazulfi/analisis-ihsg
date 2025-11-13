# indicators/rounding.py
import math
from typing import Optional, Tuple

def round_price_to_tick(price: float, tick: Optional[float], mode: str) -> float:
    """
    Round `price` to nearest multiple of `tick`.
    - mode: 'floor' or 'ceil'.
    If tick is None, 0, or non-positive -> raises ValueError.
    """
    if tick is None:
        raise ValueError("tick is None")
    try:
        tick_f = float(tick)
    except Exception:
        raise ValueError("tick invalid")
    if tick_f <= 0:
        raise ValueError("tick must be positive")
    mult = price / tick_f
    if mode == "floor":
        return float(math.floor(mult) * tick_f)
    elif mode == "ceil":
        return float(math.ceil(mult) * tick_f)
    else:
        raise ValueError("mode must be 'floor' or 'ceil'")

def enforce_tick_rounding_on_signals(
    signals_rows: list,
    tick_size: Optional[float],
    default_behavior_if_invalid: str = "no_round"  # or "warn_no_round"
) -> Tuple[list, list]:
    """
    signals_rows: list of dicts (each dict a signal row with keys sl_price, tp_price, signal_type)
    Returns (new_rows, warnings)
    - Adds keys 'sl_price_rounded' and 'tp_price_rounded' for rows where rounding applied.
    - If tick_size invalid and default_behavior_if_invalid == "no_round", just copy raw values.
    - warnings: list of strings (if any) describing rounding fallbacks.
    """
    warnings = []
    new_rows = []
    # validate tick
    try:
        if tick_size is None:
            raise ValueError("tick_size None")
        tick_f = float(tick_size)
        if tick_f <= 0:
            raise ValueError("tick_size <= 0")
    except Exception as e:
        if default_behavior_if_invalid == "no_round":
            warnings.append(f"tick_size invalid ({tick_size}) -> no rounding applied")
            for r in signals_rows:
                r2 = r.copy()
                r2['sl_price_rounded'] = r.get('sl_price')
                r2['tp_price_rounded'] = r.get('tp_price')
                new_rows.append(r2)
            return new_rows, warnings
        else:
            raise

    for r in signals_rows:
        r2 = r.copy()
        sl = r.get('sl_price')
        tp = r.get('tp_price')
        typ = (r.get('signal_type') or "").upper()
        # if sl/tp None -> keep None
        if sl is None:
            r2['sl_price_rounded'] = None
        else:
            try:
                if typ == "BUY":
                    r2['sl_price_rounded'] = round_price_to_tick(float(sl), tick_f, "floor")
                elif typ == "SELL":
                    r2['sl_price_rounded'] = round_price_to_tick(float(sl), tick_f, "ceil")
                else:
                    # unknown type -> no rounding
                    r2['sl_price_rounded'] = float(sl)
            except Exception as e:
                # fallback to raw
                r2['sl_price_rounded'] = float(sl)
                warnings.append(f"warning rounding sl for row index {r.get('index')}: {e}")

        if tp is None:
            r2['tp_price_rounded'] = None
        else:
            try:
                if typ == "BUY":
                    r2['tp_price_rounded'] = round_price_to_tick(float(tp), tick_f, "ceil")
                elif typ == "SELL":
                    r2['tp_price_rounded'] = round_price_to_tick(float(tp), tick_f, "floor")
                else:
                    r2['tp_price_rounded'] = float(tp)
            except Exception as e:
                r2['tp_price_rounded'] = float(tp)
                warnings.append(f"warning rounding tp for row index {r.get('index')}: {e}")

        new_rows.append(r2)

    return new_rows, warnings
