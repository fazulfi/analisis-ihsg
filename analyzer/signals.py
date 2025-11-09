import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")

def load_ticker(ticker):
    path = os.path.join(DATA_DIR, f"{ticker}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    return df

def add_indicators(df):
    df['ema_short'] = df['close'].ewm(span=10).mean()
    df['ema_long'] = df['close'].ewm(span=50).mean()
    delta = df['close'].diff()
    up = delta.clip(lower=0).rolling(14).mean()
    down = -delta.clip(upper=0).rolling(14).mean()
    rs = up / down
    df['rsi'] = 100 - (100 / (1 + rs))
    return df

def generate_signal(df):
    df = add_indicators(df)
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    buy = False
    if prev['ema_short'] <= prev['ema_long'] and latest['ema_short'] > latest['ema_long'] and latest['rsi'] < 70:
        buy = True
    return {'buy': bool(buy), 'rsi': float(latest['rsi'])}

if __name__ == '__main__':
    df = load_ticker("IHSG")
    print(generate_signal(df))
