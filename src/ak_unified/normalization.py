from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import math
import os
import json
import pandas as pd

_NUMERIC_FIELDS: Set[str] = {
    'open','high','low','close','last','prev_close','change','pct_change',
    'volume','amount','turnover_rate','pe','pe_ttm','pb','nav','acc_nav',
    'daily_return','yield','yield_','settlement','open_interest','iv','delta',
    'gamma','vega','theta','rho','weight'
}


def _to_date_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        ts = pd.to_datetime(v)
        return ts.strftime('%Y-%m-%d')
    except Exception:
        return None


def _to_datetime_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        ts = pd.to_datetime(v)
        return ts.isoformat()
    except Exception:
        return None


def _to_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
        if math.isfinite(f):
            return f
        return None
    except Exception:
        return None


class NormalizationRule:
    def __init__(self, prefix: str, rename_map: Optional[Dict[str, str]] = None, keep_fields: Optional[Set[str]] = None, drop_fields: Optional[Set[str]] = None) -> None:
        self.prefix = prefix
        self.rename_map = rename_map or {}
        self.keep_fields = keep_fields
        self.drop_fields = drop_fields

    def applies(self, dataset_id: str) -> bool:
        return dataset_id.startswith(self.prefix)


_DEFAULT_RULES: List[NormalizationRule] = [
    # OHLCVA equity daily/minute
    NormalizationRule(
        prefix="securities.equity.cn.ohlcva",
        keep_fields={"symbol","date","datetime","open","high","low","close","volume","amount","adjust"}
    ),
    # Index OHLCV
    NormalizationRule(
        prefix="market.index.ohlcva",
        keep_fields={"symbol","date","open","high","low","close","volume","amount"}
    ),
    # Board OHLCV
    NormalizationRule(
        prefix="securities.board.cn.",
        keep_fields=None  # allow full
    ),
    # Fund NAV (ETF/开放式)
    NormalizationRule(
        prefix="securities.fund.cn.nav",
        keep_fields={"fund_code","fund_name","nav_date","nav","acc_nav","daily_return"}
    ),
    NormalizationRule(
        prefix="securities.fund.cn.nav_open",
        keep_fields={"fund_code","fund_name","nav_date","nav","acc_nav","daily_return"}
    ),
    # Equity quotes
    NormalizationRule(
        prefix="securities.equity.",
        keep_fields=None,
        rename_map=None,
        drop_fields=None,
    ),
]


def _parse_rules_from_env() -> List[NormalizationRule]:
    raw = os.environ.get("AKU_NORMALIZATION_RULES")
    if not raw:
        return []
    try:
        data = json.loads(raw)
        rules: List[NormalizationRule] = []
        if isinstance(data, list):
            for o in data:
                if not isinstance(o, dict):
                    continue
                prefix = str(o.get("prefix") or "")
                if not prefix:
                    continue
                rename_map = o.get("rename_map") if isinstance(o.get("rename_map"), dict) else None
                keep_fields = set(o.get("keep_fields") or []) if isinstance(o.get("keep_fields"), list) else None
                drop_fields = set(o.get("drop_fields") or []) if isinstance(o.get("drop_fields"), list) else None
                rules.append(NormalizationRule(prefix, rename_map, keep_fields, drop_fields))
        return rules
    except Exception:
        return []


def _select_rule(dataset_id: str) -> Optional[NormalizationRule]:
    # longest-prefix match among env rules then defaults
    env_rules = _parse_rules_from_env()
    candidates = [r for r in env_rules if r.applies(dataset_id)] or [r for r in _DEFAULT_RULES if r.applies(dataset_id)]
    if not candidates:
        return None
    return sorted(candidates, key=lambda r: len(r.prefix), reverse=True)[0]


def _normalize_one(rec: Dict[str, Any], rule: Optional[NormalizationRule]) -> Dict[str, Any]:
    rename_map = rule.rename_map if rule else None
    keep_fields = rule.keep_fields if rule else None
    drop_fields = rule.drop_fields if rule else None

    out: Dict[str, Any] = {}
    items = list(rec.items())
    if rename_map:
        items = [(rename_map.get(str(k), str(k)), v) for k, v in items]

    for k, v in items:
        key = str(k)
        if drop_fields and key in drop_fields:
            continue
        if keep_fields is not None and key not in keep_fields:
            continue
        if key == 'date':
            ds = _to_date_str(v)
            if ds is not None:
                out['date'] = ds
            continue
        if key == 'datetime':
            ds = _to_datetime_str(v)
            if ds is not None:
                out['datetime'] = ds
            continue
        if key in ('symbol','index_symbol','board_code'):
            if isinstance(v, str):
                out[key] = v.strip().upper()
            else:
                out[key] = v
            continue
        if key in _NUMERIC_FIELDS:
            out[key] = _to_float(v)
            continue
        out[key] = v
    return out


def apply_normalization(dataset_id: str, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rule = _select_rule(dataset_id)
    return [_normalize_one(r, rule) for r in records]


def is_valid_record(dataset_id: str, rec: Dict[str, Any]) -> bool:
    # minimal validation rules by prefix
    ds = dataset_id
    # time series datasets: require time key and identifier
    if ds.endswith('ohlcva_min') or 'ohlcv_min' in ds:
        return (('symbol' in rec or 'index_symbol' in rec or 'board_code' in rec) and ('datetime' in rec))
    if 'ohlcva' in ds or 'ohlcv' in ds:
        return (('symbol' in rec or 'index_symbol' in rec or 'board_code' in rec) and ('date' in rec))
    if ds.endswith('quote'):
        return ('symbol' in rec)
    if 'constituents' in ds:
        return ('symbol' in rec and ('index_symbol' in rec or 'board_code' in rec))
    if 'fund' in ds and 'nav' in ds:
        return ('fund_code' in rec and 'nav_date' in rec)
    # default: accept
    return True


def apply_and_validate(dataset_id: str, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rule = _select_rule(dataset_id)
    out: List[Dict[str, Any]] = []
    for r in records:
        nr = _normalize_one(r, rule)
        if is_valid_record(dataset_id, nr):
            out.append(nr)
    return out