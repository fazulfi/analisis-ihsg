import os
import json
import time

# impor lokal dilakukan di dalam fungsi supaya import-time tidak error
# jika project dijalankan dari path berbeda
ROOT = os.path.dirname(os.path.dirname(__file__))
STATE_FILE = os.path.join(ROOT, "state", "last_signals.json")
os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            return json.load(open(STATE_FILE))
        except Exception:
            return {}
    return {}

def save_state(state):
    json.dump(state, open(STATE_FILE, "w"), indent=2)

def load_tickers():
    path = os.path.join(ROOT, "tickers.txt")
    if not os.path.exists(path):
        return []
    return [l.strip() for l in open(path) if l.strip()]

def check_and_send(ticker):
    # import lokal agar modul analyzer/disptacher di-resolve saat runtime
    from analyzer.signals import load_ticker, generate_signal
    from dispatcher.telegram_bot import send_signal

    df = load_ticker(ticker)
    if df is None or df.empty:
        print(f"[{ticker}] no data -> skip")
        return

    sig = generate_signal(df)
    state = load_state()
    prev = state.get(ticker)
    changed = False

    if prev is None:
        changed = True
    else:
        # kirim kalau flag buy berubah atau RSI berubah signifikan (>0.1)
        if prev.get("buy") != sig.get("buy"):
            changed = True
        elif abs(prev.get("rsi", 0) - sig.get("rsi", 0)) > 0.1:
            changed = True

    if changed:
        print(f"[{ticker}] signal changed -> sending")
        send_signal(ticker, sig)
        state[ticker] = {"buy": sig.get("buy"), "rsi": sig.get("rsi")}
        save_state(state)
    else:
        print(f"[{ticker}] no change -> skip sending")

if __name__ == "__main__":
    tickers = load_tickers()
    if not tickers:
        print("No tickers found in tickers.txt")
    for t in tickers:
        try:
            check_and_send(t)
            time.sleep(2)  # jeda kecil antar ticker untuk mengurangi risk rate-limit
        except Exception as e:
            print("Error for", t, e)
