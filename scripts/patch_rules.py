from pathlib import Path
import re
p = Path("analyzer/signal_engine/rules.py")
backup = p.with_suffix(".py.bak")
if not backup.exists():
    p.rename(backup)   # move original to backup
    backup = p.with_suffix(".py.bak")
else:
    # ensure we don't overwrite an existing backup
    print("Backup exists:", backup)

s = backup.read_text()

# pattern: find 'for i in range(n):' until 'return signals' and replace that whole block
pat = re.compile(r"\n\s*for i in range\(n\):.*?return signals\s*\n", re.S)
if not pat.search(s):
    print("Couldn't find for-loop block to replace; aborting. Showing file head:")
    print(s[:1000])
    raise SystemExit(1)

new_block = r"""
    for i in range(n):
        # If we have a cross at i, allow RSI confirmation at i or i+1
        if not crosses[i]:
            continue
        # find confirmation index: prefer i, fallback to i+1
        confirm_idx = None
        if i < n and rsi_cond[i]:
            confirm_idx = i
        elif (i + 1) < n and rsi_cond[i+1]:
            confirm_idx = i + 1
        if confirm_idx is None:
            continue
        # apply emit_next_open shift on confirmed index
        target_idx = confirm_idx + 1 if emit_next_open else confirm_idx
        if target_idx >= n:
            continue
        ts = df.iloc[target_idx][ts_col] if ts_col and ts_col in df.columns else df.index[target_idx]
        price = float(df.iloc[target_idx][price_col])
        signals.append({"index": target_idx, "ts": ts, "signal": "BUY", "price": price})
    return signals
"""

s2 = pat.sub("\n" + new_block + "\n", s, count=1)
p.write_text(s2)
print("Patched analyzer/signal_engine/rules.py — backup at", backup)
