from __future__ import annotations

import json
import os
from typing import Dict

import yaml  # type: ignore


def load_account_key_map(default_map: Dict[str, str]) -> Dict[str, str]:
    path = os.environ.get("AKU_ACCOUNT_MAP")
    if not path:
        return default_map
    try:
        if path.endswith(('.yml', '.yaml')):
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
        else:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        if not isinstance(data, dict):
            return default_map
        merged = dict(default_map)
        for k, v in data.items():
            if isinstance(k, str) and isinstance(v, str):
                merged[k] = v
        return merged
    except Exception:
        return default_map