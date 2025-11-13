#!/usr/bin/env python3
"""
scripts/run_pipeline.py (timestamp-aware)
Usage:
  python scripts/run_pipeline.py <TICKER> [--data DATA/<TICKER>.csv] [--signals SIGNALS.csv] [--config config.yaml] [--timestamp-col TIMESTAMP_COL] [--append]
"""
import sys
from pathlib import Path
import json

def load_config(path="config.yaml"):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError("config file not found: " + path)
    ext = p.suffix.lower()
    txt = p.read_text(encoding="utf-8")
    if ext in (".yaml", ".yml"):
        import yaml
        return yaml.safe_load(txt)
    else:
        return json.loads(txt)

def format_num(v):
    if v is None or v == "":
        return ""
    try:
        return f"{float(v):.2f}"
    except Exception:
        return str(v)

def ensure_date_column(df, timestamp_col=None):
    import pandas as pd
    cols_lower = {c.lower(): c for c in df.columns}
    # if already has date, nothing to do
    if 'date' in cols_lower:
        return df
    # if timestamp_col provided and exists (case-insensitive), use it
    if timestamp_col:
        tcol = None
        for k,v in cols_lower.items():
            if k == timestamp_col.lower():
                tcol = v
                break
        if tcol:
            ts = df[tcol]
            # numeric epoch?
            if pd.api.types.is_integer_dtype(ts) or pd.api.types.is_float_dtype(ts):
                sample = int(ts.dropna().iloc[0]) if ts.dropna().shape[0] > 0 else 0
                unit = 's' if sample < 1e11 else 'ms'
                if unit == 'ms':
                    df['date'] = pd.to_datetime(df[tcol], unit='ms', errors='coerce').dt.strftime('%Y-%m-%d')
                else:
                    df['date'] = pd.to_datetime(df[tcol], unit='s', errors='coerce').dt.strftime('%Y-%m-%d')
            else:
                df['date'] = pd.to_datetime(df[tcol], errors='coerce').dt.strftime('%Y-%m-%d')
            return df
        # else fallthrough to try common names
    # try common timestamp-like names
    for trycol in ('timestamp','time','datetime','date_time','trading_date'):
        if trycol in cols_lower:
            colname = cols_lower[trycol]
            ts = df[colname]
            if pd.api.types.is_integer_dtype(ts) or pd.api.types.is_float_dtype(ts):
                sample = int(ts.dropna().iloc[0]) if ts.dropna().shape[0] > 0 else 0
                unit = 's' if sample < 1e11 else 'ms'
                if unit == 'ms':
                    df['date'] = pd.to_datetime(df[colname], unit='ms', errors='coerce').dt.strftime('%Y-%m-%d')
                else:
                    df['date'] = pd.to_datetime(df[colname], unit='s', errors='coerce').dt.strftime('%Y-%m-%d')
            else:
                df['date'] = pd.to_datetime(df[colname], errors='coerce').dt.strftime('%Y-%m-%d')
            return df
    # nothing to do
    return df

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_pipeline.py <TICKER> [--data DATA/<TICKER>.csv] [--signals SIGNALS.csv] [--config config.yaml] [--timestamp-col TIMESTAMP_COL] [--append]")
        sys.exit(2)

    ticker = sys.argv[1]
    args = sys.argv[2:]
    data_path = Path(f"DATA/{ticker}.csv")
    signals_csv = None
    cfg_path = "config.yaml"
    append_mode = False
    timestamp_col = None

    if "--data" in args:
        i = args.index("--data")
        data_path = Path(args[i+1])

    if "--signals" in args:
        i = args.index("--signals")
        signals_csv = Path(args[i+1])

    if "--config" in args:
        i = args.index("--config")
        cfg_path = args[i+1]

    if "--append" in args:
        append_mode = True

    if "--timestamp-col" in args:
        i = args.index("--timestamp-col")
        if i+1 < len(args):
            timestamp_col = args[i+1]
        else:
            print("ERROR: --timestamp-col requires a value"); sys.exit(2)

    # load config
    try:
        cfg = load_config(cfg_path)
    except Exception as e:
        print("ERROR loading config:", e)
        sys.exit(2)

    atr_period = int(cfg.get("atr_period", 14))
    sl_mult = float(cfg.get("sl_multiplier", 1.5))
    tp_mult = float(cfg.get("tp_multiplier", 3.0))
    tick_size = cfg.get("tick_size", None)
    entry_src = cfg.get("entry_price_source", "close")

    # import helpers
    import pandas as pd
    from indicators.atr import compute_tr_and_atr
    from signal_engine.integration import attach_atr_and_entry_to_signals
    from indicators.sltp import compute_sltp_for_signal
    from indicators.rounding import enforce_tick_rounding_on_signals

    # read data
    if not data_path.exists():
        print("ERROR: data file not found:", data_path)
        sys.exit(3)

    df = pd.read_csv(data_path)
    colmap = {c.lower(): c for c in df.columns}
    df.columns = [c.lower() for c in df.columns]

    # if date missing, attempt to create using timestamp_col or known names
    if 'date' not in df.columns:
        df = ensure_date_column(df, timestamp_col=timestamp_col)
        if 'date' not in df.columns:
            print("ERROR: data missing required column: date (or convertible timestamp). Use --timestamp-col if your timestamp has a different name.")
            sys.exit(3)

    # validate other required cols
    for c in ("open","high","low","close","volume"):
        if c not in df.columns:
            print("ERROR: data missing required column:", c)
            sys.exit(3)

    # compute tr & atr
    df_tr_atr = compute_tr_and_atr(df, atr_period=atr_period)

    # signals: try provided or default signals/<TICKER>_signals.csv
    if signals_csv is None:
        candidate = Path("signals") / f"{ticker}_signals.csv"
        if candidate.exists():
            signals_csv = candidate

    signals = None
    if signals_csv:
        if not signals_csv.exists():
            print("ERROR: signals CSV not found:", signals_csv)
            sys.exit(4)
        sig_df = pd.read_csv(signals_csv)
        sig_df.columns = [c.lower() for c in sig_df.columns]
        signals = []
        if 'index' in sig_df.columns:
            for _, r in sig_df.iterrows():
                signals.append({"index": int(r['index']) if not pd.isna(r['index']) else None, "signal_type": r.get('signal_type', None), "date": r.get('date', None)})
        elif 'date' in sig_df.columns:
            df_dates = df_tr_atr['date'].astype(str).tolist()
            for _, r in sig_df.iterrows():
                d = str(r['date'])
                if d in df_dates:
                    idx = df_dates.index(d)
                    signals.append({"index": idx, "signal_type": r.get('signal_type', None), "date": d})
                else:
                    signals.append({"index": None, "signal_type": r.get('signal_type', None), "date": d, "note": "signal_date_not_in_data"})
        else:
            print("ERROR: signals CSV must have 'index' or 'date' column")
            sys.exit(4)
    else:
        try:
            from signal_engine import signals as signal_gen_mod
            if hasattr(signal_gen_mod, "generate_signals"):
                signals = signal_gen_mod.generate_signals(df_tr_atr)
            else:
                print("ERROR: no signals provided and generator not found. Provide --signals signals/<TICKER>_signals.csv")
                sys.exit(4)
        except Exception:
            print("ERROR: no signals provided and cannot import generator. Provide --signals signals/<TICKER>_signals.csv")
            sys.exit(4)

    # attach atr & entry
    attached = attach_atr_and_entry_to_signals(df_tr_atr, signals, cfg=cfg)

    from signal_engine.postprocess import enforce_single_open_signal

    # setelah attach
    attached_filtered, attached_skipped = enforce_single_open_signal(attached, df_tr_atr, cfg=cfg)
    # proceed using attached_filtered (instead of attached)
    # You may want to log attached_skipped or include them in output with notes
    attached = attached_filtered

    # compute sl/tp
    out_rows = []
    for s in attached:
        row = {}
        idx = s.get("index")
        if idx is None:
            date = s.get("date", "")
        else:
            date = str(df_tr_atr.loc[idx, "date"])
        row['date'] = date
        row['signal_type'] = s.get("signal_type", "")
        entry = s.get("entry_price", None)
        atrv = s.get("atr_value", None)
        row['entry_price'] = entry
        row['atr_value'] = atrv
        row['atr_period'] = atr_period
        row['sl_multiplier'] = sl_mult
        row['tp_multiplier'] = tp_mult
        row['entry_price_source'] = entry_src
        note = s.get("note", None)
        if atrv is None:
            slp = None; tpp = None
            if note is None:
                note = "insufficient_data_for_atr"
        else:
            slp, tpp, note2 = compute_sltp_for_signal(entry, atrv, sl_mult, tp_mult, tick_size, s.get("signal_type","BUY"), min_positive_tick=tick_size)
            if note2:
                if note:
                    note = f"{note};{note2}"
                else:
                    note = note2
        row['sl_price'] = slp
        row['tp_price'] = tpp
        row['notes'] = note or ""
        out_rows.append(row)

    # rounding enforcement
    rounded_rows, warnings = enforce_tick_rounding_on_signals(out_rows, tick_size, default_behavior_if_invalid="no_round")

    import pandas as pd
    out_df = pd.DataFrame(rounded_rows)
    cols = ['date','signal_type','entry_price','atr_value','sl_price','tp_price','sl_price_rounded','tp_price_rounded','atr_period','sl_multiplier','tp_multiplier','entry_price_source','notes']
    for c in cols:
        if c not in out_df.columns:
            out_df[c] = ""
    out_df = out_df[cols]

    for c in ['entry_price','atr_value','sl_price','tp_price','sl_price_rounded','tp_price_rounded']:
        out_df[c] = out_df[c].apply(lambda x: format_num(x) if x != "" and not pd.isna(x) else "")

    out_path = Path("OUTPUT") / f"{ticker}_signals.csv"
    mode = 'a' if append_mode else 'w'
    header = not (append_mode and out_path.exists())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, mode=mode, header=header, index=False)
    print("OK: pipeline done ->", out_path)
    if warnings:
        print("Warnings:")
        for w in warnings:
            print(" -", w)

if __name__ == "__main__":
    main()
