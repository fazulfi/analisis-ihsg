# analyzer/indicators.py
"""
Indicator helpers untuk project.
Berisi fungsi untuk menambahkan EMA ke DataFrame OHLCV.
"""

from typing import Iterable, Tuple, Sequence
import pandas as pd


def add_ema(df: pd.DataFrame, spans: Sequence[int] = (9, 21, 50), price_col: str = "close") -> pd.DataFrame:
    """
    Tambahkan kolom EMA ke DataFrame.

    Args:
        df: DataFrame yang minimal berisi kolom `close` (atau nama price_col).
            Indeks boleh datetime atau integer.
        spans: iterable of int, period EMA yang ingin dihitung (misal (9,21,50)).
        price_col: nama kolom harga yang dipakai (default: "close").

    Returns:
        DataFrame yang sama (di-place) dengan tambahan kolom `ema_{span}` untuk tiap span.
    """
    if price_col not in df.columns:
        raise ValueError(f"price_col '{price_col}' tidak ditemukan di DataFrame")

    # Pastikan bekerja pada salinan jika user mau (kita modifikasi in-place sesuai plan)
    for s in spans:
        if not isinstance(s, int) or s <= 0:
            raise ValueError("spans harus iterable berisi integer > 0")
        col_name = f"ema_{s}"
        # adjust=False -> common konvensi Wilder-like smoothing for EMA as typical in trading libs
        df[col_name] = df[price_col].ewm(span=s, adjust=False).mean()
    return df


def add_ema_many(df: pd.DataFrame, spans: Iterable[int], price_col: str = "close") -> pd.DataFrame:
    """
    Alias/konvenience function — sama seperti add_ema, tapi eksplisit menerima iterable.
    """
    return add_ema(df, spans=tuple(spans), price_col=price_col)

# GANTI fungsi add_rsi dengan kode ini di analyzer/indicators.py
import numpy as np
import pandas as pd
from typing import Any

def add_rsi(df: pd.DataFrame, period: int = 14, price_col: str = "close") -> pd.DataFrame:
    """
    RSI klasik (Wilder) dengan seeding yang menjaga agar pada uptrend murni
    nilai awal avg_loss bukan absolut 0 sehingga RSI dapat meningkat secara bertahap.
    """
    if price_col not in df.columns:
        raise ValueError(f"price_col '{price_col}' tidak ditemukan di DataFrame")

    close = df[price_col].astype(float)
    delta = close.diff().to_numpy()

    n = len(close)
    if n == 0:
        df[f"rsi_{period}"] = pd.Series(dtype=float)
        return df

    # gain / loss arrays
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)

    avg_gain = np.full(n, np.nan, dtype=float)
    avg_loss = np.full(n, np.nan, dtype=float)

    if n > period:
        # seed: simple average of first `period` deltas (indices 1..period)
        first_avg_gain = gain[1: period + 1].mean()
        first_avg_loss = loss[1: period + 1].mean()

        # If there were no losses in the seed but there are gains,
        # initialize a small non-zero loss proportional to avg_gain (e.g. 10%)
        # so RSI warms up instead of jumping to 100 immediately.
        if first_avg_loss == 0.0 and first_avg_gain > 0.0:
            first_avg_loss = first_avg_gain * 0.10  # 10% heuristic

        avg_gain[period] = first_avg_gain
        avg_loss[period] = first_avg_loss

        # Wilder recursive smoothing
        for i in range(period + 1, n):
            avg_gain[i] = (avg_gain[i - 1] * (period - 1) + gain[i]) / period
            avg_loss[i] = (avg_loss[i - 1] * (period - 1) + loss[i]) / period

    # compute RSI where avg_gain defined
    rsi = np.full(n, np.nan, dtype=float)
    valid_idx = np.where(~np.isnan(avg_gain))[0]
    for i in valid_idx:
        ag = avg_gain[i]
        al = avg_loss[i]
        if al == 0.0:
            if ag == 0.0:
                rsi[i] = 50.0
            else:
                # If avg_loss eventually becomes 0 despite seed, treat as very high RSI
                rsi[i] = 100.0
        else:
            rs = ag / al
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))

    col_name = f"rsi_{period}"
    df[col_name] = pd.Series(rsi, index=df.index).clip(lower=0.0, upper=100.0)
    return df

# analyzer/indicators.py (tambahkan fungsi ini)

from typing import Optional
import pandas as pd
import numpy as np

def add_macd(df: pd.DataFrame,
             fast: int = 12,
             slow: int = 26,
             signal: int = 9,
             price_col: str = "close") -> pd.DataFrame:
    """
    Tambahkan kolom macd, macd_signal, macd_hist ke DataFrame.
    - macd = EMA(fast) - EMA(slow)
    - macd_signal = EMA(signal) of macd
    - macd_hist = macd - macd_signal

    Fungsi memodifikasi df in-place dan juga mengembalikan df.
    """
    if price_col not in df.columns:
        raise ValueError(f"price_col '{price_col}' tidak ditemukan di DataFrame")

    # pastikan kolom numeric
    series = df[price_col].astype(float)

    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=signal, adjust=False).mean()
    macd_hist = macd_line - macd_signal

    df["macd"] = macd_line
    df["macd_signal"] = macd_signal
    df["macd_hist"] = macd_hist

    return df

# tambahkan/impor di analyzer/indicators.py
import pandas as pd

def add_atr(df: pd.DataFrame, period: int = 14, high_col: str = "high", low_col: str = "low", close_col: str = "close") -> pd.DataFrame:
    """
    Tambahkan kolom 'atr_{period}' ke DataFrame menggunakan Wilder smoothing (alpha=1/period).
    - Menghitung True Range (TR) = max(high-low, |high - prev_close|, |low - prev_close|)
    - ATR = TR.ewm(alpha=1/period, adjust=False).mean()
    """
    # cek kolom ada
    for col in (high_col, low_col, close_col):
        if col not in df.columns:
            raise ValueError(f"Kolom {col} tidak ditemukan di DataFrame")

    high = df[high_col].astype(float)
    low = df[low_col].astype(float)
    prev_close = df[close_col].shift(1).astype(float)

    high_low = high - low
    high_close = (high - prev_close).abs()
    low_close = (low - prev_close).abs()

    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1.0/period, adjust=False).mean()

    col_name = f"atr_{period}"
    df[col_name] = atr
    return df

# analyzer/indicators.py
from typing import Mapping, Any, Iterable, Sequence

def _maybe_add_column(df: pd.DataFrame, colname: str, generator_fn, force: bool = False):
    """
    Helper: panggil generator_fn() untuk menghasilkan kolom hanya jika:
      - kolom tidak ada, OR
      - force == True
    generator_fn harus mengembalikan Series atau array yang cocok indexnya.
    """
    if (colname in df.columns) and not force:
        # jangan timpa
        return df
    values = generator_fn()
    # pastikan index alignment
    ser = pd.Series(values, index=df.index)
    df[colname] = ser
    return df


def add_all_indicators(df: pd.DataFrame, config: Mapping[str, Any] = None, force: bool = False) -> pd.DataFrame:
    """
    Tambahkan indikator utama ke DataFrame sesuai `config`.
    - df: DataFrame yg minimal punya kolom 'close' (dan 'high','low' untuk ATR)
    - config: dict opsional, contoh:
        {
          "ema_spans": (9,21,50),
          "rsi_period": 14,
          "macd": {"fast":12, "slow":26, "signal":9},
          "atr_period": 14
        }
    - force: jika True, akan menimpa kolom indikator yg sudah ada

    Returns: df yang sama (dimodifikasi in-place) dengan kolom:
      - ema_{span} untuk setiap span
      - rsi_{period}
      - macd, macd_signal, macd_hist
      - atr_{period}
    """
    cfg = dict(config or {})

    # EMA (bisa banyak spans)
    ema_spans = tuple(cfg.get("ema_spans", (9, 21, 50)))
    # Tambah tiap EMA jika belum ada (atau force)
    for s in ema_spans:
        col = f"ema_{s}"
        def gen_ema(s=s):
            return df['close'].astype(float).ewm(span=s, adjust=False).mean()
        _maybe_add_column(df, col, lambda s=s: gen_ema(s), force=force)

    # RSI
    rsi_period = int(cfg.get("rsi_period", 14))
    rsi_col = f"rsi_{rsi_period}"
    if rsi_col not in df.columns or force:
        add_rsi(df, period=rsi_period, price_col='close')  # function mutates df

    # MACD
    macd_cfg = dict(cfg.get("macd", {"fast": 12, "slow": 26, "signal": 9}))
    # compute only if missing or forced
    if ("macd" not in df.columns) or force:
        add_macd(df, fast=int(macd_cfg.get("fast", 12)), slow=int(macd_cfg.get("slow", 26)), signal=int(macd_cfg.get("signal", 9)), price_col='close')

    # ATR
    atr_period = int(cfg.get("atr_period", 14))
    atr_col = f"atr_{atr_period}"
    if atr_col not in df.columns or force:
        add_atr(df, period=atr_period, high_col='high', low_col='low', close_col='close')

    return df
