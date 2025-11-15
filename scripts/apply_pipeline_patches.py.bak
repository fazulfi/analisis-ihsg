#!/usr/bin/env python3
"""
apply_pipeline_patches.py

Patch otomatis:
1) buat imports internal (mis. `from indicators.atr import ...`) lebih robust:
   - coba import normal dulu
   - jika gagal, tambahkan parent dir ke sys.path lalu import ulang
2) ubah default timestamp arg menjadi 'timestamp' (ganti default 'date' -> 'timestamp')
   untuk argumen CLI seperti: --timestamp-col / timestamp_col default value.
3) backup file asli ke <file>.bak sebelum modifikasi.

Jalankan: python scripts/apply_pipeline_patches.py
"""

import io
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # repo root
TARGET_DIRS = [
    ROOT / "scripts",
    ROOT / "analyzer",
    ROOT / "analyzer" / "signal_engine",
    ROOT / "indicators",
    ROOT / "signal_engine",
]

# patterns for imports to make robust
IMPORT_RE = re.compile(r"^(from\s+)(indicators(?:\.[\w_]+)*)\s+import\s+(.+)$", re.M)
# also detect "from analyzer.indicators..." or other top-level indicator-like imports
IMPORT_RE2 = re.compile(r"^(from\s+)(analyzer\.indicators(?:\.[\w_]+)*)\s+import\s+(.+)$", re.M)

# pattern to change default timestamp arg (two forms commonly used)
TIMESTAMP_ARG_RE1 = re.compile(r"(--timestamp-col'\s*,\s*default=)('date'|\"date\")")
TIMESTAMP_ARG_RE2 = re.compile(r"(add_argument\(\s*['\"]--timestamp-col['\"].*?default\s*=\s*)(['\"][^'\"]+['\"])", re.S)

# helper: produce robust import wrapper string
def make_robust_import(module_path: str, imports: str) -> str:
    """
    Given module_path like "indicators.atr" and imports like "compute_tr_and_atr",
    returns a snippet that tries normal import then falls back by adding parent dir.
    """
    tpl = (
        "try:\n"
        "    from {module} import {what}\n"
        "except Exception:\n"
        "    # fallback when running as script (adjust sys.path to include repo root)\n"
        "    import sys, os\n"
        "    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))\n"
        "    if repo_root not in sys.path:\n"
        "        sys.path.insert(0, repo_root)\n"
        "    try:\n"
        "        from {module} import {what}\n"
        "    except Exception:\n"
        "        # last resort: try top-level import without package prefix\n"
        "        modname = '{module}'.split('.')[-1]\n"
        "        from importlib import import_module\n"
        "        im = import_module(modname)\n"
        "        for name in [{what_literals}]:\n"
        "            globals()[name] = getattr(im, name)\n"
    )
    # prepare what_literals: split by comma and quote
    what_items = [w.strip() for w in imports.split(",")]
    what_literals = ", ".join("'" + w + "'" for w in what_items)
    return tpl.format(module=module_path, what=imports, what_literals=what_literals)

def backup_file(p: Path):
    bak = p.with_suffix(p.suffix + ".bak")
    if not bak.exists():
        p.replace(p) if False else None  # noop to avoid linter issues
    # create copy
    with p.open("rb") as fr, bak.open("wb") as fw:
        fw.write(fr.read())

def patch_file(p: Path):
    text = p.read_text(encoding="utf-8")
    orig = text

    changed = False

    # 1) patch imports like: from indicators.something import a, b
    def repl_import(m):
        nonlocal changed
        whole = make_robust_import(m.group(2), m.group(3))
        changed = True
        return whole
    text = IMPORT_RE.sub(repl_import, text)
    text = IMPORT_RE2.sub(repl_import, text)

    # 2) change default timestamp args to 'timestamp' if there is a default 'date'
    # Attempt a few common patterns: parser.add_argument('--timestamp-col', default='date', ...)
    if "timestamp" not in text or "default='timestamp'" in text:
        # replace explicit default='date' occurrences
        new_text = TIMESTAMP_ARG_RE1.sub(r"\1' timestamp '", text)
        # fallback: try a looser replacement for add_argument default values
        new_text = TIMESTAMP_ARG_RE2.sub(lambda m: m.group(1) + "'timestamp'", new_text)
        if new_text != text:
            text = new_text
            changed = True

    # 3) ensure file-level top comment for patched imports (optional)
    if changed and "AUTOPATCHED_BY_apply_pipeline_patches" not in text:
        hdr = "# AUTOPATCHED_BY_apply_pipeline_patches: added robust imports and timestamp default\n"
        text = hdr + text

    if changed:
        # backup and write
        bak = p.with_suffix(p.suffix + ".bak")
        if not bak.exists():
            bak.write_bytes(orig.encode("utf-8"))
        p.write_text(text, encoding="utf-8")
        print(f"Patched: {p}")
    else:
        print(f"No changes: {p}")

def iter_targets():
    # collect .py files under target dirs
    files = []
    for d in TARGET_DIRS:
        if not d.exists():
            continue
        for p in d.rglob("*.py"):
            # skip __pycache__ and hidden
            if "__pycache__" in p.parts:
                continue
            files.append(p)
    # also include scripts/*.py in root scripts directory
    sdir = ROOT / "scripts"
    if sdir.exists():
        for p in sdir.glob("*.py"):
            if "__pycache__" in p.parts:
                continue
            files.append(p)
    # uniq and sort
    seen = set()
    out = []
    for f in sorted(files):
        if str(f) not in seen:
            out.append(f)
            seen.add(str(f))
    return out

def main():
    files = iter_targets()
    print(f"Found {len(files)} python files to check.")
    for p in files:
        try:
            patch_file(p)
        except Exception as e:
            print(f"Error patching {p}: {e}")

if __name__ == "__main__":
    main()
