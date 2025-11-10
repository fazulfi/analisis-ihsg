# -- ganti main() di runner/check_and_dispatch.py dengan ini --

import os
import pandas as pd
from analyzer.indicators import add_all_indicators
from analyzer.signals import generate_signals
from dispatcher.telegram_bot import dispatch_signals

def _read_historical_csv(path="data/historical.csv"):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    # timestamp adalah kolom ke-2 pada CSV kamu (index 1), jadi parse_dates=[1]
    df = pd.read_csv(path, parse_dates=[1], index_col=1)
    # pastikan index datetime
    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            print("[WARN] Could not convert index to DatetimeIndex; leaving as-is.")
    return df

def _ensure_symbol_column(df, tickers_path="tickers.txt"):
    if "symbol" in df.columns:
        print(f"[INFO] symbol column detected: {df['symbol'].nunique()} unique")
        return df
    if "ticker" in df.columns:
        df["symbol"] = df["ticker"]
        print("[INFO] symbol column filled from 'ticker' column.")
        return df
    if os.path.exists(tickers_path):
        try:
            with open(tickers_path, "r", encoding="utf-8") as f:
                tickers = [line.strip() for line in f if line.strip()]
            if len(tickers) == 1:
                df["symbol"] = tickers[0]
                print(f"[INFO] symbol column set to single ticker from {tickers_path}: {tickers[0]}")
                return df
        except Exception:
            pass
    df["symbol"] = "UNKNOWN"
    print("[WARN] No 'symbol' or 'ticker' column found and tickers.txt ambiguous – setting symbol='UNKNOWN'.")
    return df

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Dry-run mode (no real Telegram send)")
    args = parser.parse_args()

    # --- Load sample or real data ---
    path = "data/historical.csv"
    df = _read_historical_csv(path)
    print(f"[INFO] Loaded {len(df)} rows from {path}")

    # ensure symbol column exists (helpful for dispatcher)
    df = _ensure_symbol_column(df, tickers_path="tickers.txt")

    # --- Add indicators ---
    config = {
        "ema_spans": [9, 21, 50],
        "rsi_period": 14,
        "macd": {"fast": 12, "slow": 26, "signal": 9},
        "atr_period": 14
    }
    df = add_all_indicators(df, config=config)

    # --- Generate signals ---
    df = generate_signals(df)
    buy_count = int((df["signal"] == "buy").sum())
    print(f"[INFO] Generated {buy_count} buy signals")

    # --- Dispatch ---
    bot_token = os.environ.get("TG_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TG_CHAT_ID", "YOUR_CHAT_ID")
    dispatched = dispatch_signals(df, bot_token, chat_id, dry_run=args.test)
    print(f"[INFO] Dispatched {dispatched} new signals.")

if __name__ == "__main__":
    main()
