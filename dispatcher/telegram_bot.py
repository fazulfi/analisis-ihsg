# dispatcher/telegram_bot.py
import os
import json
import requests
from datetime import datetime

STATE_FILE = "state/last_signals.json"


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def send_telegram_message(bot_token, chat_id, text, dry_run=False):
    """Kirim pesan ke Telegram atau tampilkan ke stdout kalau test mode"""
    if dry_run:
        print(f"[TEST] Telegram message → {chat_id}: {text}")
        return True

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": chat_id, "text": text}
    resp = requests.post(url, data=data)
    if resp.status_code != 200:
        print(f"[WARN] Telegram send failed: {resp.text}")
        return False
    return True


def dispatch_signals(df, bot_token, chat_id, dry_run=False):
    """
    Kirim sinyal baru ke Telegram berdasarkan kolom 'signal' dan 'symbol' (jika ada).
    Hindari duplikasi dengan file state/last_signals.json
    """
    state = load_state()
    last_sent = state.get("sent", [])

    new_signals = []
    for idx, row in df.iterrows():
        if str(row.get("signal", "none")).lower() != "buy":
            continue
        symbol = row.get("symbol", "UNKNOWN")
        timestamp = str(idx)
        key = f"{symbol}_{timestamp}"
        if key in last_sent:
            continue

        msg = f"📈 BUY Signal: {symbol}\nTime: {timestamp}\nPrice: {row.get('close', 'N/A')}"
        if send_telegram_message(bot_token, chat_id, msg, dry_run=dry_run):
            new_signals.append(key)

    if new_signals:
        state.setdefault("sent", []).extend(new_signals)
        save_state(state)

    print(f"✅ Dispatched {len(new_signals)} new signals.")
    return len(new_signals)
