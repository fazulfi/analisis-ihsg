import sqlite3
import pandas as pd
from analyzer.signals import add_indicators  # pakai fungsi existing untuk hitung EMA/RSI

def backtest_ticker(ticker):
    con = sqlite3.connect("data/historical.db")
    df = pd.read_sql(f"SELECT * FROM prices WHERE ticker='{ticker}' ORDER BY date", con, parse_dates=['date'])
    con.close()
    if df.empty:
        print("no data", ticker); return
    df.set_index('date', inplace=True)
    # normalisasi nama kolom (pastikan 'close' ada)
    df.columns = [c.lower() for c in df.columns]
    # pastikan kolom close numeric
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df = add_indicators(df)  # harus mengembalikan kolom 'ema_short','ema_long','rsi'
    # simple rule: buy if ema_short > ema_long AND rsi < 30
    df['position'] = 0
    df.loc[(df['ema_short'] > df['ema_long']) & (df['rsi'] < 30), 'position'] = 1
    df['signal'] = df['position'].diff().fillna(0)
    buys = df[df['signal'] == 1]
    sells = df[df['signal'] == -1]
    print(f"Backtest {ticker}: rows={len(df)}, buys={len(buys)}, sells={len(sells)}")
    if not buys.empty:
        print("Sample buys (last 5):")
        print(buys[['close','ema_short','ema_long','rsi']].tail(5))
    print("-"*50)

if __name__ == '__main__':
    tickers = ['BBCA.JK','TLKM.JK']  # ganti sesuai mau
    for t in tickers:
        backtest_ticker(t)
