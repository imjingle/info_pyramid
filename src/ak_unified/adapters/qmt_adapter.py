from __future__ import annotations

import os
import platform
from typing import Any, Dict, Tuple, Optional

import pandas as pd

from ..config import load_account_key_map  # not used here but kept for parity


class QmtAdapterError(RuntimeError):
    pass


def is_windows() -> bool:
    return platform.system().lower() == 'windows'


def _ensure_windows() -> None:
    if not is_windows():
        raise QmtAdapterError(
            "QMT adapter is Windows-only. Please run on Windows with QMT/ThinkTrader client and native API installed."
        )


def _import_qmt_module() -> Any:
    mod_name = os.environ.get('AKU_QMT_PYMOD')
    candidates = [mod_name] if mod_name else ['qmt', 'qmt_native', 'thinktrader', 'qmtapi']
    last_err: Optional[Exception] = None
    for name in candidates:
        if not name:
            continue
        try:
            return __import__(name)
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            continue
    raise QmtAdapterError(f"Failed to import QMT Python module from candidates {candidates}: {last_err}")


def _get_mapping() -> Dict[str, str]:
    default = {
        'ohlcv_daily': 'get_kline_daily',
        'ohlcv_min': 'get_kline_min',
        'quote': 'get_realtime_quote',
        'calendar': 'get_trade_calendar',
        'adjust_factor': 'get_adjust_factor',
        'board_industry': 'get_board_industry_constituents',
        'board_concept': 'get_board_concept_constituents',
        'index_constituents': 'get_index_constituents',
        'corporate_actions': 'get_corporate_actions',
    }
    path = os.environ.get('AKU_QMT_CONFIG')
    if not path:
        return default
    try:
        import json, yaml  # type: ignore
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) if path.endswith(('.yml', '.yaml')) else json.load(f)
        if isinstance(data, dict) and 'qmt' in data and isinstance(data['qmt'], dict):
            merged = dict(default)
            merged.update({k: v for k, v in data['qmt'].items() if isinstance(k, str) and isinstance(v, str)})
            return merged
    except Exception:
        return default
    return default


def get_qmt_mapping() -> Dict[str, str]:
    return _get_mapping()


def test_qmt_import() -> Dict[str, Any]:
    try:
        _ensure_windows()
        mod = _import_qmt_module()
        mapping = _get_mapping()
        avail = {k: hasattr(mod, v) for k, v in mapping.items()}
        return {"ok": True, "module": mod.__name__, "mapping": mapping, "available": avail}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e), "is_windows": is_windows()}


def _to_dataframe(obj: Any) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, list):
        return pd.DataFrame(obj)
    if isinstance(obj, dict):
        return pd.DataFrame([obj])
    return pd.DataFrame([])


def call_qmt(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    _ensure_windows()
    qmt = _import_qmt_module()
    mapping = _get_mapping()

    if dataset_id.endswith('ohlcv_daily'):
        fn_name = mapping['ohlcv_daily']
        fn = getattr(qmt, fn_name, None)
        if fn is None:
            raise QmtAdapterError(f"QMT function not found: {fn_name}")
        df = _to_dataframe(fn(symbol=params.get('symbol'), start=params.get('start'), end=params.get('end')))
        if not df.empty:
            rename = {'日期': 'date', '时间': 'datetime', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume', '成交额': 'amount'}
            df = df.rename(columns={**rename, **{c: c for c in df.columns}})
            if 'symbol' not in df.columns and params.get('symbol'):
                df.insert(0, 'symbol', params.get('symbol'))
        return (fn_name, df)

    if dataset_id.endswith('ohlcv_min'):
        fn_name = mapping['ohlcv_min']
        fn = getattr(qmt, fn_name, None)
        if fn is None:
            raise QmtAdapterError(f"QMT function not found: {fn_name}")
        df = _to_dataframe(fn(symbol=params.get('symbol'), start=params.get('start'), end=params.get('end'), freq=params.get('freq')))
        if not df.empty:
            rename = {'日期': 'date', '时间': 'datetime', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume', '成交额': 'amount'}
            df = df.rename(columns={**rename, **{c: c for c in df.columns}})
            if 'symbol' not in df.columns and params.get('symbol'):
                df.insert(0, 'symbol', params.get('symbol'))
        return (fn_name, df)

    if dataset_id.endswith('quote'):
        fn_name = mapping['quote']
        fn = getattr(qmt, fn_name, None)
        df = _to_dataframe(fn()) if fn else pd.DataFrame([])
        return (fn_name, df)

    if dataset_id == 'market.calendar.qmt':
        fn_name = mapping['calendar']
        fn = getattr(qmt, fn_name, None)
        df = _to_dataframe(fn(start=params.get('start'), end=params.get('end'))) if fn else pd.DataFrame([])
        if not df.empty:
            df = df.rename(columns={'日期': 'date', 'is_trading_day': 'is_trading_day'})
        return (fn_name, df)

    if dataset_id == 'securities.equity.cn.adjust_factor.qmt':
        fn_name = mapping['adjust_factor']
        fn = getattr(qmt, fn_name, None)
        df = _to_dataframe(fn(symbol=params.get('symbol'), start=params.get('start'), end=params.get('end'))) if fn else pd.DataFrame([])
        if not df.empty:
            df = df.rename(columns={'代码': 'symbol', '日期': 'date', '复权因子': 'adjust_factor'})
        return (fn_name, df)

    if dataset_id in ('securities.board.cn.industry.qmt', 'securities.board.cn.concept.qmt'):
        key = 'board_industry' if 'industry' in dataset_id else 'board_concept'
        fn_name = mapping[key]
        fn = getattr(qmt, fn_name, None)
        df = _to_dataframe(fn()) if fn else pd.DataFrame([])
        if not df.empty:
            df = df.rename(columns={'板块名称': 'board_name', '代码': 'symbol', '名称': 'symbol_name', '权重': 'weight'})
        return (fn_name, df)

    if dataset_id == 'market.index.constituents.qmt':
        fn_name = mapping['index_constituents']
        fn = getattr(qmt, fn_name, None)
        df = _to_dataframe(fn(index_code=params.get('index_code') or params.get('symbol'))) if fn else pd.DataFrame([])
        if not df.empty:
            df = df.rename(columns={'指数代码': 'index_symbol', '代码': 'symbol', '名称': 'symbol_name', '权重': 'weight', '日期': 'date'})
        return (fn_name, df)

    if dataset_id == 'securities.equity.cn.corporate_actions.qmt':
        fn_name = mapping['corporate_actions']
        fn = getattr(qmt, fn_name, None)
        df = _to_dataframe(fn(symbol=params.get('symbol'))) if fn else pd.DataFrame([])
        if not df.empty:
            df = df.rename(columns={'公告日期': 'record_date', '权息类型': 'action_type', '除权除息日': 'ex_date', '发放日': 'payable_date', '现金分红': 'cash_dividend', '送股比例': 'stock_dividend_ratio', '拆分比例': 'split_ratio', '代码': 'symbol'})
        return (fn_name, df)

    return ('qmt.unsupported', pd.DataFrame([]))