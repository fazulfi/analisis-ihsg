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

# backtest/simple_backtest.py
"""
Simple backtester untuk sinyal 'buy' saja.

- Input: DataFrame dengan index datetime dan kolom:
    ['open','high','low','close','volume', 'signal', 'atr_14' (atau atr_N)]
- Logika:
    - Saat menemukan bar dengan signal == 'buy', buka posisi LONG pada next bar open (if exists)
    - TP = entry_price + tp_atr_mult * atr
    - SL = entry_price - sl_atr_mult * atr
    - Per bar sampai posisi close, cek high >= TP or low <= SL (first hit closes)
    - If none hit until end, close at last available close (market close)
- Output: list of trades + summary stats
"""

from typing import List, Dict, Any, Tuple
import pandas as pd
import math


def simple_backtest(df: pd.DataFrame,
                    signal_col: str = "signal",
                    entry_on_next_open: bool = True,
                    atr_col_prefix: str = "atr_",
                    atr_period: int = 14,
                    tp_atr_mult: float = 2.0,
                    sl_atr_mult: float = 1.0) -> Dict[str, Any]:
    """
    Run a buy-only backtest.

    Returns dict with:
      - trades: list of trade dicts
      - summary: {n_trades, wins, losses, winrate, total_pnl, avg_pnl}
    """
    if df.empty:
        return {"trades": [], "summary": {}}

    df = df.copy()
    # choose atr column name
    atr_col = f"{atr_col_prefix}{atr_period}"
    if atr_col not in df.columns:
        # fallback: try any column starting with atr_
        atr_cols = [c for c in df.columns if c.startswith(atr_col_prefix)]
        atr_col = atr_cols[0] if atr_cols else None

    trades: List[Dict[str, Any]] = []
    idxs = list(df.index)

    for i, ts in enumerate(idxs):
        row = df.loc[ts]
        if row.get(signal_col, None) != "buy":
            continue

        entry_idx = i + 1 if entry_on_next_open else i
        if entry_idx >= len(idxs):
            # cannot enter (no next bar)
            continue

        entry_ts = idxs[entry_idx]
        entry_open = df.at[entry_ts, "open"]

        # ATR value at entry bar (prefer entry_ts row)
        atr_val = None
        if atr_col and atr_col in df.columns:
            atr_val = df.at[entry_ts, atr_col]
        # fallback: small epsilon
        if atr_val is None or (isinstance(atr_val, float) and math.isnan(atr_val)):
            atr_val = 0.001 * entry_open  # tiny volatility if no ATR

        tp = entry_open + tp_atr_mult * float(atr_val)
        sl = entry_open - sl_atr_mult * float(atr_val)

        # walk subsequent bars from entry_idx..end until closed
        closed = False
        exit_price = None
        exit_ts = None
        reason = None
        for j in range(entry_idx, len(idxs)):
            t2 = idxs[j]
            h = float(df.at[t2, "high"])
            l = float(df.at[t2, "low"])
            c = float(df.at[t2, "close"])
            # check TP first (take-profit)
            if h >= tp:
                exit_price = tp
                exit_ts = t2
                reason = "tp"
                closed = True
                break
            if l <= sl:
                exit_price = sl
                exit_ts = t2
                reason = "sl"
                closed = True
                break
        if not closed:
            # close at last close available
            exit_ts = idxs[-1]
            exit_price = float(df.at[exit_ts, "close"])
            reason = "close_end"

        pnl = exit_price - entry_open  # long trade
        trades.append({
            "entry_ts": entry_ts,
            "entry_price": entry_open,
            "exit_ts": exit_ts,
            "exit_price": exit_price,
            "tp": tp,
            "sl": sl,
            "atr": atr_val,
            "pnl": pnl,
            "reason": reason
        })

    # summary
    n = len(trades)
    wins = sum(1 for t in trades if t["pnl"] > 0)
    losses = sum(1 for t in trades if t["pnl"] <= 0)
    total_pnl = sum(t["pnl"] for t in trades)
    avg_pnl = (total_pnl / n) if n else 0.0
    winrate = (wins / n) if n else 0.0

    summary = {
        "n_trades": n,
        "wins": wins,
        "losses": losses,
        "winrate": winrate,
        "total_pnl": total_pnl,
        "avg_pnl": avg_pnl
    }
    return {"trades": trades, "summary": summary}
