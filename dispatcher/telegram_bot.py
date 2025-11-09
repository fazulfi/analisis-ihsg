import os
from telegram import Bot

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_message(text):
    if not TOKEN or not CHAT_ID:
        print("TELEGRAM env vars not set. Message not sent. Text:", text)
        return
    bot = Bot(token=TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=text)
    print("Message sent.")

def send_signal(ticker, signal):
    text = f"Sinyal untuk {ticker}:\\nBuy: {signal['buy']}\\nRSI: {signal.get('rsi'):.2f}"
    send_message(text)

if __name__ == '__main__':
    send_message("Bot test: aktif.")
