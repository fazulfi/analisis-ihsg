#!/usr/bin/env python3
import sys, os, json

try:
    import yaml
except Exception:
    yaml = None

REQUIRED_FIELDS = {
    "atr_period": int,
    "sl_multiplier": (int, float),
    "tp_multiplier": (int, float),
    "tick_size": (int, float, type(None)),
    "atr_method": str,
    "entry_price_source": str,
    "output_folder": str,
    "data_folder": str
}

def load_config(path):
    ext = os.path.splitext(path)[1].lower()
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    if ext in [".yaml", ".yml"]:
        if yaml is None:
            raise RuntimeError("ERROR: PyYAML belum diinstall. Install pakai: pip install pyyaml")
        return yaml.safe_load(text)
    elif ext == ".json":
        return json.loads(text)
    else:
        raise RuntimeError("ERROR: Format config tidak didukung (harus .yaml/.yml atau .json).")

def validate(cfg):
    missing = []
    wrong_type = []
    for k, t in REQUIRED_FIELDS.items():
        if k not in cfg:
            missing.append(k)
        else:
            if not isinstance(cfg[k], t):
                wrong_type.append((k, type(cfg[k]).__name__, getattr(t, "__name__", str(t))))
    return missing, wrong_type

def main():
    if len(sys.argv) < 2:
        print("Usage: python validate_config.py <config.yaml|config.json>")
        sys.exit(2)

    path = sys.argv[1]
    if not os.path.exists(path):
        print(f"ERROR: file tidak ditemukan → {path}")
        sys.exit(2)

    try:
        cfg = load_config(path)
    except Exception as e:
        print("ERROR saat load config:", e)
        sys.exit(2)

    missing, wrong_type = validate(cfg)

    if missing or wrong_type:
        print("CONFIG VALIDATION FAILED ❌")
        if missing:
            print(" - Missing fields:", ", ".join(missing))
        if wrong_type:
            for k, got, exp in wrong_type:
                print(f" - Field '{k}' tipe salah: got {got}, expected {exp}")
        sys.exit(3)

    print("CONFIG VALIDATION PASSED ✅")
    print("\nConfig summary:")
    for k in REQUIRED_FIELDS.keys():
        print(f"  {k}: {cfg.get(k)!r}")

if __name__ == "__main__":
    main()
