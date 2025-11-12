# analyzer/signal_engine/config.py
"""
Konfigurasi utama untuk rule EMA + RSI (spot only).
Semua nilai dapat diimpor oleh modul lain.
"""

# --- EMA parameter ---
EMA_FAST = 9       # periode EMA cepat
EMA_SLOW = 21      # periode EMA lambat

# --- RSI parameter ---
RSI_PERIOD = 14
RSI_BUY_THRESHOLD = 30  # sinyal valid jika RSI > 30
RSI_SLOPE_FILTER = True # aktifkan filter RSI naik

# --- Mode sinyal ---
SIGNAL_MODE = "BUY_ONLY"  # khusus spot, hanya sinyal beli
SIGNAL_LABEL = "EMA_CROSS_RSI_BUY"

# --- Metadata ---
VERSION = "1.0.0"
DESCRIPTION = "Kombinasi EMA cross bullish + RSI filter (spot, buy only)"
