# ingestor/storage.py
"""
Storage helpers untuk project Analisis Saham IHSG.

Fungsi utama:
- normalize_ohlcv(data) -> DataFrame  (step 1)
- append_to_csv(rows, path)           (dipakai di tests)
- save_to_sqlite(rows, dbpath, table) (dipakai di tests; idempotent upsert)

File ini menerima input sebagai:
- pandas.DataFrame, atau
- list of dicts (biasa di fixtures tests)
"""

from pathlib import Path
import sqlite3
import pandas as pd
from typing import Union, Iterable, Any, List

# header/kolom yang diharapkan tests
FIELDNAMES = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'source']


# ---------------------------
# Utility: standardize columns
# ---------------------------
def _standardize_colnames(df: pd.DataFrame) -> pd.DataFrame:
    """Map variasi nama kolom ke canonical names kami (non-destructive)."""
    mapping = {}
    for c in list(df.columns):
        lc = c.lower().strip()
        if 'symbol' in lc or lc in ('ticker', 'code'):
            mapping[c] = 'symbol'
        elif 'timestamp' in lc or ('time' in lc and 'stamp' in lc):
            mapping[c] = 'timestamp'
        elif lc in ('datetime', 'date', 'time'):
            mapping[c] = 'datetime'
        elif lc in ('o', 'open', 'open_price'):
            mapping[c] = 'open'
        elif lc in ('h', 'high', 'high_price'):
            mapping[c] = 'high'
        elif lc in ('l', 'low', 'low_price'):
            mapping[c] = 'low'
        elif lc in ('c', 'close', 'close_price', 'adj close', 'adjclose'):
            mapping[c] = 'close'
        elif 'vol' in lc:
            mapping[c] = 'volume'
        elif 'source' in lc:
            mapping[c] = 'source'
    if mapping:
        return df.rename(columns=mapping)
    return df


# ---------------------------
# Utility: timestamp formatter
# ---------------------------
def _to_iso_utc_with_colon(ts_series: pd.Series) -> pd.Series:
    """
    Convert pandas datetime-like series to ISO strings with timezone +00:00.
    We convert to UTC to have consistent output like: 2025-11-09T00:00:00+00:00
    """
    # to UTC (handles tz-aware and naive)
    dt_utc = pd.to_datetime(ts_series, errors='coerce', utc=True)
    # strftime gives +0000, convert to +00:00 by inserting colon
    s = dt_utc.dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    # handle NaT -> NaN (keep as pd.NA)
    s = s.where(~dt_utc.isna(), other=pd.NA)
    # convert +0000 -> +00:00 (works for other offsets too)
    def fix_tz(x: Any) -> Any:
        if pd.isna(x):
            return pd.NA
        if len(x) >= 5 and (x[-5] in ('+', '-')) and x[-3] != ':':
            # convert +HHMM -> +HH:MM
            return x[:-5] + x[-5:-2] + ":" + x[-2:]
        return x
    return s.apply(fix_tz)


# -----------------------------------------
# Normalize input rows into canonical format
# -----------------------------------------
def _rows_to_dataframe(rows: Union[pd.DataFrame, Iterable[dict]]) -> pd.DataFrame:
    """
    Normalize input (DataFrame or iterable-of-dicts) into DataFrame
    with columns exactly FIELDNAMES (in that order), and:
    - ensure numeric columns are numeric
    - ensure timestamp column exists and is ISO string with timezone
    - drop rows missing required keys (symbol, timestamp, close)
    """
    # create df
    if isinstance(rows, pd.DataFrame):
        df = rows.copy()
    else:
        # If rows is generator, convert to list first
        df = pd.DataFrame(list(rows))

    if df.empty:
        # return an empty DataFrame with FIELDNAMES if nothing provided
        return pd.DataFrame(columns=FIELDNAMES)

    # standardize column names first (map common variants)
    df = _standardize_colnames(df)

    # if datetime is present but timestamp not, we will convert later
    if isinstance(df.index, pd.DatetimeIndex) and 'timestamp' not in df.columns and 'datetime' not in df.columns:
        df = df.reset_index().rename(columns={'index': 'datetime'})

    # ensure all FIELDNAMES exist
    for col in FIELDNAMES:
        if col not in df.columns:
            df[col] = pd.NA

    # If timestamp missing but datetime exists -> create timestamp from datetime
    if (df['timestamp'].isna().all() or df['timestamp'].dtype == object) and 'datetime' in df.columns:
        # convert datetime-like to ISO UTC strings
        df['timestamp'] = _to_iso_utc_with_colon(df['datetime'])
    else:
        # normalize whatever is in timestamp column to ISO UTC
        df['timestamp'] = _to_iso_utc_with_colon(df['timestamp'])

    # Coerce numeric columns
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # normalize symbol/source as strings (trim)
    df['symbol'] = df['symbol'].astype(str).str.strip().replace({'nan': pd.NA})
    df['source'] = df['source'].astype(str).str.strip().replace({'nan': pd.NA})

    # Reorder columns to FIELDNAMES and drop rows missing essential fields
    df = df[FIELDNAMES]
    df = df.dropna(subset=['symbol', 'timestamp', 'close'])

    # Ensure timestamp is str (tests compare str equality)
    df['timestamp'] = df['timestamp'].astype(str)

    return df


# ---------------------------
# Public: normalize_ohlcv
# ---------------------------
def normalize_ohlcv(data: Union[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Legacy helper for OHLCV-style data (keperluan step 1).
    Accepts:
      - path to CSV (str) or pandas.DataFrame
    Returns:
      - DataFrame indexed by datetime with columns open,high,low,close,volume
    NOTE: This is kept for step-1 compatibility. For tests that expect symbol/timestamp
    use append_to_csv/save_to_sqlite which call _rows_to_dataframe.
    """
    if isinstance(data, str):
        df = pd.read_csv(data)
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        raise ValueError("data must be path to CSV or pandas.DataFrame")

    df = _standardize_colnames(df)

    if 'datetime' not in df.columns:
        # try to use index
        try:
            idx = pd.to_datetime(df.index)
            df = df.reset_index().rename(columns={'index': 'datetime'})
        except Exception:
            raise ValueError("No datetime column found")

    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime'])
    df = df.set_index('datetime').sort_index()

    # ensure required columns exist
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col not in df.columns:
            df[col] = pd.NA
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['volume'] = df['volume'].fillna(0)
    return df[['open', 'high', 'low', 'close', 'volume']]


# ---------------------------
# Public: append_to_csv
# ---------------------------
def append_to_csv(rows: Union[pd.DataFrame, Iterable[dict]], path: str):
    """
    Append rows (DataFrame or iterable-of-dicts) to CSV file.
    - Creates parent folder if needed.
    - Writes header (FIELDNAMES) if file did not exist.
    """
    csv_path = Path(path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    df = _rows_to_dataframe(rows)
    # if empty after normalization, do nothing
    if df.empty:
        return

    write_header = not csv_path.exists()
    if write_header:
        df.to_csv(csv_path, index=False, columns=FIELDNAMES, encoding='utf-8')
    else:
        df.to_csv(csv_path, index=False, mode='a', header=False, columns=FIELDNAMES, encoding='utf-8')


# ---------------------------
# Public: save_to_sqlite (idempotent upsert)
# ---------------------------
def save_to_sqlite(rows: Union[pd.DataFrame, Iterable[dict]], dbpath: str, table: str = "historical"):
    """
    Save rows to sqlite DB with composite PK (symbol, timestamp).
    - Creates DB file & parent folders if needed.
    - Uses INSERT OR REPLACE to achieve idempotent upsert behavior.
    """
    db_file = Path(dbpath)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    df = _rows_to_dataframe(rows)
    if df.empty:
        return

    conn = sqlite3.connect(str(db_file))
    cur = conn.cursor()

    # create table schema expected by tests
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            source TEXT,
            PRIMARY KEY (symbol, timestamp)
        );
    """)
    insert_sql = f"""
        INSERT OR REPLACE INTO {table}
        (symbol, timestamp, open, high, low, close, volume, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    params: List[tuple] = []
    for _, r in df.iterrows():
        params.append((
            r['symbol'],
            r['timestamp'],
            float(r['open']) if pd.notna(r['open']) else None,
            float(r['high']) if pd.notna(r['high']) else None,
            float(r['low']) if pd.notna(r['low']) else None,
            float(r['close']) if pd.notna(r['close']) else None,
            float(r['volume']) if pd.notna(r['volume']) else None,
            r['source'] if pd.notna(r['source']) else None
        ))
    if params:
        cur.executemany(insert_sql, params)
        conn.commit()
    conn.close()
