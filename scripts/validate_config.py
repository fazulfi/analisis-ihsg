#!/usr/bin/env python3
"""
scripts/validate_config.py
Usage:
  python scripts/validate_config.py config.yaml

Validates required fields and types for pipeline config.
Exits 0 if OK, 2 if invalid.
"""
import sys
import os
import json
import yaml

REQUIRED_KEYS = {
    "atr_period": int,
    "sl_multiplier": (int, float),
    "tp_multiplier": (int, float),
    "tick_size": (int, float),
    "atr_method": str,
    "entry_price_source": str
}

ALLOWED = {
    "atr_method": {"wilder", "sma"},
    "entry_price_source": {"close", "next_open"}
}

def load_config(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    ext = os.path.splitext(path)[1].lower()
    with open(path, "r", encoding="utf-8") as f:
        if ext in (".yaml", ".yml"):
            return yaml.safe_load(f)
        elif ext == ".json":
            return json.load(f)
        else:
            raise ValueError("Unsupported config format. Use .yaml/.yml or .json")

def validate_config(cfg):
    errors = []
    if not isinstance(cfg, dict):
        errors.append("Config must be a mapping/object at top level.")
        return errors
    for k, expected_type in REQUIRED_KEYS.items():
        if k not in cfg:
            errors.append(f"Missing required key: '{k}'")
            continue
        v = cfg[k]
        if not isinstance(v, expected_type):
            errors.append(f"Invalid type for '{k}': expected {expected_type}, got {type(v).__name__}")
    # value checks
    for key, allowed in ALLOWED.items():
        if key in cfg and cfg[key] not in allowed:
            errors.append(f"Invalid value for '{key}': '{cfg[key]}' not in {sorted(list(allowed))}")
    # numeric sanity checks
    if "atr_period" in cfg and isinstance(cfg.get("atr_period"), int) and cfg["atr_period"] <= 0:
        errors.append("atr_period must be > 0")
    if "sl_multiplier" in cfg and cfg["sl_multiplier"] <= 0:
        errors.append("sl_multiplier must be > 0")
    if "tp_multiplier" in cfg and cfg["tp_multiplier"] <= 0:
        errors.append("tp_multiplier must be > 0")
    if "tick_size" in cfg and cfg["tick_size"] <= 0:
        errors.append("tick_size must be > 0")
    return errors

def main(argv):
    if len(argv) < 2:
        print("Usage: python scripts/validate_config.py <config.yaml|config.json>", file=sys.stderr)
        sys.exit(2)
    path = argv[1]
    try:
        cfg = load_config(path)
    except Exception as e:
        print(f"ERROR loading config: {e}", file=sys.stderr)
        sys.exit(2)
    errs = validate_config(cfg)
    if errs:
        print("CONFIG INVALID:", file=sys.stderr)
        for e in errs:
            print(" -", e, file=sys.stderr)
        sys.exit(2)
    print("CONFIG OK")
    # optionally print normalized config
    # print(json.dumps(cfg, indent=2))
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))

