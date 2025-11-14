# tests/test_config_validator.py
import tempfile
import os
import json
import yaml
from scripts.validate_config import load_config, validate_config

GOOD = {
    "atr_period": 14,
    "sl_multiplier": 1.5,
    "tp_multiplier": 3.0,
    "tick_size": 5,
    "atr_method": "wilder",
    "entry_price_source": "close"
}

def test_validate_good_yaml(tmp_path):
    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(GOOD))
    cfg = load_config(str(p))
    errs = validate_config(cfg)
    assert errs == []

def test_validate_missing_key_json(tmp_path):
    bad = dict(GOOD)
    del bad["atr_period"]
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(bad))
    cfg = load_config(str(p))
    errs = validate_config(cfg)
    assert any("Missing required key" in e for e in errs)

def test_validate_invalid_values(tmp_path):
    bad = dict(GOOD)
    bad["atr_period"] = -1
    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(bad))
    cfg = load_config(str(p))
    errs = validate_config(cfg)
    assert any("atr_period must be > 0" in e for e in errs)
