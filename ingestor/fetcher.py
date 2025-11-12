# ingestor/fetcher.py
"""
Robust fetcher:
- read tickers from tickers.txt
- fetch each ticker via yfinance
- normalize columns and index
- save one CSV per ticker to data/tickers/<SYM>.csv
- optionally write combined CSV and parquet (parquet requires pyarrow or fastparquet)
"""
from __future__ import annotations
import os
from pathlib import Path
import time
import logging
from typing import List, Optional

import pandas as pd
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def read_tickers(path: str | Path = "tickers.txt") -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{path} not found")
    with p.open() as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """flatten MultiIndex columns like ('Close','BBCA.JK') -> 'Close_BBCA.JK'"""
    cols = []
    for c in df.columns:
        if isinstance(c, tuple):
            # join non-empty parts with underscore
            cols.append("_".join([str(x) for x in c if (x is not None and str(x) != "")]).strip())
        else:
            cols.append(str(c))
    df = df.copy()
    df.columns = cols
    return df


def _normalize_df(raw: pd.DataFrame, symbol: str) -> Optional[pd.DataFrame]:
    """
    Normalize single-yf dataframe into columns:
    timestamp(index), open, high, low, close, volume, source
    Returns None if essential cols missing.
    """
    if raw is None or raw.empty:
        return None

    df = _flatten_columns(raw)

    # possible names (yfinance can produce MultiIndex, or single-column). Try to find close/open/high/low/volume.
    # prefer non-adjusted close if available; fallback to 'Adj Close' if needed
    # The typical patterns after flatten: 'Open_BBCA.JK', 'Close_BBCA.JK', 'Adj Close_BBCA.JK', 'Volume_BBCA.JK'
    # we'll search for columns ending with the symbol or containing it.
    suffix = symbol
    candidates = {c: c for c in df.columns}

    # helper to pick column by possible names
    def pick(pref_names):
        for nm in pref_names:
            # direct match
            if nm in df.columns:
                return nm
        # suffix match
        for c in df.columns:
            for nm in pref_names:
                if c.endswith(f"_{nm}") or c.endswith(nm):
                    return c
        # contains symbol and key word
        for c in df.columns:
            for nm in pref_names:
                if nm.replace(" ", "").lower() in c.replace("_", "").lower():
                    return c
        return None

    close_col = pick(["Close", "Adj Close", "AdjClose"])
    open_col = pick(["Open"])
    high_col = pick(["High"])
    low_col = pick(["Low"])
    vol_col = pick(["Volume", "Vol"])

    # if close is not found, give up
    if close_col is None or open_col is None or high_col is None or low_col is None or vol_col is None:
        logging.warning("normalize_yf_df: missing essential cols for %s -> skipping (found: %s)", symbol, list(df.columns))
        return None

    # ensure index is datetime
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            logging.warning("could not parse index to datetime for %s", symbol)
            return None

    # timezone handling: ensure tz-aware or localized then strip tz (store naive UTC timestamps)
    try:
        if df.index.tz is None:
            # localize to UTC (yfinance sometimes returns tz-naive but times are UTC)
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
        # convert to tz-naive (for easier parquet compatibility)
        df.index = df.index.tz_localize(None)
    except Exception as e:
        logging.warning("tz handling failed for %s: %s", symbol, e)
        # try best-effort: leave index as-is
        pass

    out = pd.DataFrame(
        {
            "symbol": symbol,
            "timestamp": df.index,
            "open": pd.to_numeric(df[open_col], errors="coerce"),
            "high": pd.to_numeric(df[high_col], errors="coerce"),
            "low": pd.to_numeric(df[low_col], errors="coerce"),
            "close": pd.to_numeric(df[close_col], errors="coerce"),
            "volume": pd.to_numeric(df[vol_col], errors="coerce"),
            "source": "yfinance",
        },
        index=df.index,
    )
    # drop rows where close is NaN
    out = out.dropna(subset=["close"])
    if out.empty:
        return None
    # keep timestamp as column (index also)
    out["timestamp"] = out.index
    return out


def fetch_single(symbol: str, period: str = "90d", interval: str = "1d", retries: int = 3, pause: float = 1.0) -> Optional[pd.DataFrame]:
    for attempt in range(1, retries + 1):
        try:
            logging.info("Will fetch %s (attempt %d)", symbol, attempt)
            raw = yf.download(symbol, period=period, interval=interval, progress=False, auto_adjust=False)
            if raw is None or raw.empty:
                logging.debug("no frames returned for %s", symbol)
                raise RuntimeError("no frames")
            norm = _normalize_df(raw, symbol)
            if norm is None or norm.empty:
                raise RuntimeError("no valid normalized frames")
            return norm
        except Exception as e:
            logging.warning("fetch %s attempt %d failed: %s", symbol, attempt, e)
            time.sleep(pause * attempt)
    logging.error("no frames fetched for %s", symbol)
    return None


def fetch_stocks_from_list(
    tickers: List[str],
    period: str = "90d",
    interval: str = "1d",
    per_ticker_folder: str = "data/tickers",
    combined_path: str = "data/historical.csv",
    to_parquet: bool = False,
    parquet_path: str = "data/historical.parquet",
) -> pd.DataFrame:
    per_ticker_folder = Path(per_ticker_folder)
    per_ticker_folder.mkdir(parents=True, exist_ok=True)
    combined_frames = []
    for sym in tickers:
        res = fetch_single(sym, period=period, interval=interval)
        if res is None:
            continue
        # save per-ticker CSV (append mode)
        out_path = per_ticker_folder / f"{sym}.csv"
        res_to_save = res.reset_index(drop=True)
        try:
            res_to_save.to_csv(out_path, index=False)
            logging.info("Saved %s rows to %s", len(res_to_save), out_path)
        except Exception as e:
            logging.warning("failed saving per-ticker csv for %s: %s", sym, e)
        combined_frames.append(res_to_save)

    if not combined_frames:
        logging.info("Saved 0 total rows to %s (no valid frames)", per_ticker_folder)
        # create empty combined dataframe
        empty = pd.DataFrame(columns=["symbol", "timestamp", "open", "high", "low", "close", "volume", "source"])
        return empty

    combined = pd.concat(combined_frames, ignore_index=True)
    # sort by symbol + timestamp
    combined = combined.sort_values(["symbol", "timestamp"]).reset_index(drop=True)

    combined_parent = Path(combined_path).parent
    combined_parent.mkdir(parents=True, exist_ok=True)
    # write CSV
    combined.to_csv(combined_path, index=False)
    logging.info("Saved %s rows to %s", len(combined), combined_path)

    # optionally write parquet (requires pyarrow or fastparquet)
    if to_parquet:
        try:
            combined.to_parquet(parquet_path, index=False, compression="snappy")
            logging.info("Saved combined parquet to %s", parquet_path)
        except Exception as e:
            logging.warning("failed to write parquet (%s). Install pyarrow or fastparquet. Error: %s", parquet_path, e)

    return combined


def main():
    tickers = []
    try:
        tickers = read_tickers("tickers.txt")
    except FileNotFoundError:
        logging.error("tickers.txt not found in repo root")
        return

    combined = fetch_stocks_from_list(tickers, period="90d", interval="1d", per_ticker_folder="data/tickers", combined_path="data/historical.csv", to_parquet=False)
    logging.info("Done. combined rows: %d", len(combined))


if __name__ == "__main__":
    main()
