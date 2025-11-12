#!/usr/bin/env python3
"""
scripts/backtest_sanity.py

Simple manual backtest / sanity check:
- Load OHLC (CSV path given) OR download via yfinance if ticker provided and internet available.
- Compute indicators via project functions and generate signals.
- Save plot and a small markdown report.
"""
import os
import sys
import argparse
from datetime import datetime, timedelta
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# use project modules
from analyzer.signal_engine.rules import generate_signals
from analyzer.indicators import add_all_indicators

def load_data(csv_path: str = None, ticker: str = None, period_days: int = 7):
    if csv_path:
        df = pd.read_csv(csv_path, parse_dates=True)
        # try to detect common column names
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        elif "Date" in df.columns:
            df["timestamp"] = pd.to_datetime(df["Date"])
        else:
            # try index as date
            df = df.reset_index().rename(columns={"index": "timestamp"})
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        # ensure columns: timestamp, open, high, low, close, volume maybe
        return df
    if ticker:
        try:
            import yfinance as yf
        except Exception as e:
            raise RuntimeError("yfinance not installed or no internet. Provide CSV path instead.") from e
        end = datetime.utcnow()
        start = end - timedelta(days=period_days)
        data = yf.download(ticker, start=start.date(), end=end.date(), interval="1m" if period_days<=2 else "15m")
        if data.empty:
            raise RuntimeError("No data fetched for ticker=" + ticker)
        data = data.reset_index().rename(columns={"Datetime": "timestamp"})
        return data
    raise RuntimeError("Provide csv_path or ticker")

def plot_with_signals(df, ef_col, es_col, signals, price_col="close", ts_col="timestamp", out_path="reports/plots/signal_sanity.png"):
    plt.figure(figsize=(14,6))
    x = pd.to_datetime(df[ts_col])
    plt.plot(x, df[price_col], label="close", linewidth=1.25)
    if ef_col in df.columns:
        plt.plot(x, df[ef_col], label=ef_col, linewidth=1)
    if es_col in df.columns:
        plt.plot(x, df[es_col], label=es_col, linewidth=1)
    # plot buy markers
    buys_x = []
    buys_y = []
    for s in signals:
        buys_x.append(pd.to_datetime(s["ts"]))
        buys_y.append(s["price"])
    if buys_x:
        plt.scatter(buys_x, buys_y, marker="^", color="green", s=100, label="BUY")
    plt.legend()
    plt.title("Price + EMAs + BUY signals")
    plt.xlabel("Time")
    plt.ylabel("Price")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close()

def summarize_signals(signals):
    if not signals:
        return "No BUY signals generated.\n"
    lines = []
    lines.append(f"Total BUY signals: {len(signals)}")
    for s in signals:
        ts = pd.to_datetime(s["ts"])
        lines.append(f"- {ts.isoformat()} price={s['price']:.4f}")
    return "\n".join(lines) + "\n"

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", help="path to OHLC CSV (must include timestamp/open/high/low/close)", default=None)
    parser.add_argument("--ticker", help="yfinance ticker to download (optional)", default=None)
    parser.add_argument("--days", help="days to download when using ticker", type=int, default=7)
    parser.add_argument("--cfg", help="config as python literal (eg \"{'ema_spans':(9,21),'rsi_period':14}\")", default="{'ema_spans': (9,21), 'rsi_period': 14}")
    parser.add_argument("--emit-next-open", action="store_true", help="emit signals for next open bar")
    parser.add_argument("--out-md", default="reports/signal_sanity.md")
    parser.add_argument("--out-plot", default="reports/plots/signal_sanity.png")
    args = parser.parse_args(argv)

    try:
        cfg = eval(args.cfg)
    except Exception:
        print("Invalid cfg format", file=sys.stderr)
        sys.exit(1)

    df = load_data(csv_path=args.csv, ticker=args.ticker, period_days=args.days)
    # normalize column names to lowercase
    df.columns = [c.lower() for c in df.columns]
    # choose price column
    if "close" not in df.columns:
        raise RuntimeError("close column not found in data")
    if "timestamp" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "timestamp"})
    if "timestamp" not in df.columns:
        # if index is datetime
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={"index":"timestamp"})
        else:
            # create synthetic timestamp
            df["timestamp"] = pd.date_range("2025-01-01", periods=len(df), freq="min")

    # ensure indicators exist (this will compute ema_x and rsi_x columns)
    cfg2 = dict(cfg or {})
    if ('high' not in df.columns) or ('low' not in df.columns):
        # skip ATR computation when OHLC not provided
        cfg2['atr_period'] = 0
    add_all_indicators(df, cfg2, force=True)

    short, long = tuple(cfg.get("ema_spans", (9,21)))
    ef_col = f"ema_{short}"
    es_col = f"ema_{long}"
    rsi_col = f"rsi_{cfg.get('rsi_period', 14)}"

    signals = generate_signals(df.copy(), cfg=cfg, price_col="close", ts_col="timestamp", force_indicators=False, emit_next_open=args.emit_next_open)

    # plot and report
    plot_with_signals(df, ef_col, es_col, signals, price_col="close", ts_col="timestamp", out_path=args.out_plot)
    summary = summarize_signals(signals)
    # basic sanity heuristics
    reasons = []
    if not signals:
        reasons.append("No signals found — either no momentum in sample or config spans too large for sample length.")
    else:
        # compute distribution: percent of bars that have signals
        pct = len(signals) / max(1, len(df)) * 100.0
        if pct > 5.0:
            reasons.append(f"High signal density: {pct:.2f}% of bars are BUY → consider this too many.")
        else:
            reasons.append(f"Signal density {pct:.2f}% — reasonable for short sample.")

    md = []
    md.append("# Signal Sanity Report")
    md.append(f"Generated: {datetime.utcnow().isoformat()} UTC")
    md.append("")
    md.append("## Config")
    md.append(f"```json\n{cfg}\n```")
    md.append("")
    md.append("## Summary")
    md.append(summary)
    md.append("## Heuristics / Comments")
    for r in reasons:
        md.append(f"- {r}")
    md.append("")
    md.append("## Plot")
    md.append(f"![plot]({os.path.basename(args.out_plot)})")
    md_text = "\n\n".join(md)
    os.makedirs(os.path.dirname(args.out_md), exist_ok=True)
    with open(args.out_md, "w", encoding="utf8") as f:
        f.write(md_text)

    print("Report written to", args.out_md)
    print("Plot written to", args.out_plot)
    print("Summary:")
    print(summary)

if __name__ == "__main__":
    main()
