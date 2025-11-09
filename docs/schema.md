Schema data historis:
- symbol (TEXT)
- timestamp (TEXT, ISO8601)  # primary key bersama symbol
- open (REAL)
- high (REAL)
- low (REAL)
- close (REAL)
- volume (INTEGER)
- source (TEXT)
Primary key: (symbol, timestamp)
