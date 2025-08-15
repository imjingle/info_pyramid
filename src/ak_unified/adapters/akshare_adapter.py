from __future__ import annotations

import importlib
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from ..logging import logger
from ..rate_limiter import acquire_rate_limit


class AkAdapterError(RuntimeError):
    pass


def _import_akshare():
    try:
        return importlib.import_module("akshare")
    except Exception as exc:
        raise AkAdapterError("Failed to import akshare. Ensure dependency is installed.") from exc


def _rename_columns(df: pd.DataFrame, field_mapping: Optional[Dict[str, str]]) -> pd.DataFrame:
    if field_mapping:
        cols = {c: field_mapping.get(c, c) for c in df.columns}
        df = df.rename(columns=cols)
    return df


def _ensure_symbol_column(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    symbol = params.get("symbol")
    if symbol and "symbol" not in df.columns:
        df.insert(0, "symbol", symbol)
    return df


def _normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if c in {"date", "datetime"}:
            continue
        try:
            df[c] = pd.to_numeric(df[c])
        except Exception:
            pass
    return df


async def _call_single(ak_module, fn_name: str, params: Dict[str, Any]) -> pd.DataFrame:
    fn = getattr(ak_module, fn_name, None)
    if not callable(fn):
        raise AkAdapterError(f"AkShare function not found: {fn_name}")
    
    # Get vendor for rate limiting
    vendor = ak_function_vendor(fn_name)
    
    # Acquire rate limit before making call
    await acquire_rate_limit('akshare', vendor)
    
    call_params = {k: v for k, v in params.items() if v is not None}
    data = fn(**call_params) if call_params else fn()
    return data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)


def ak_function_vendor(fn_name: str) -> str:
    name = fn_name.lower()
    if name.endswith('_em') or 'eastmoney' in name:
        return 'eastmoney'
    if name.endswith('_sina') or 'sina' in name:
        return 'sina'
    if name.endswith('_tx') or 'tencent' in name or name.endswith('_qq'):
        return 'tencent'
    if 'ths' in name or name.endswith('_ths'):
        return 'ths'
    if 'tdx' in name:
        return 'tdx'
    if 'baidu' in name:
        return 'baidu'
    if 'netease' in name or '163' in name:
        return 'netease'
    if 'hexun' in name:
        return 'hexun'
    if 'csindex' in name:
        return 'csindex'
    if 'jsl' in name or 'jisilu' in name:
        return 'jisilu'
    logger.warning(f"ak_function_vendor: unknown vendor for function: {fn_name}")
    return 'unknown'


async def call_akshare(
    ak_functions: List[str],
    params: Dict[str, Any],
    field_mapping: Optional[Dict[str, str]] = None,
    allow_fallback: bool = False,
    function_name: Optional[str] = None,
) -> Tuple[str, pd.DataFrame]:
    ak = _import_akshare()

    if function_name:
        df = await _call_single(ak, function_name, params)
        df = _rename_columns(df, field_mapping)
        df = _ensure_symbol_column(df, params)
        df = _normalize_types(df)
        return function_name, df

    if not allow_fallback:
        if len(ak_functions) == 1:
            fn = ak_functions[0]
            df = await _call_single(ak, fn, params)
            df = _rename_columns(df, field_mapping)
            df = _ensure_symbol_column(df, params)
            df = _normalize_types(df)
            return fn, df
        else:
            raise AkAdapterError(f"Multiple functions specified but fallback not allowed: {ak_functions}")

    # Try each function in order until one succeeds
    for fn in ak_functions:
        try:
            df = await _call_single(ak, fn, params)
            if not df.empty:
                df = _rename_columns(df, field_mapping)
                df = _ensure_symbol_column(df, params)
                df = _normalize_types(df)
                return fn, df
        except Exception as e:
            logger.warning(f"AkShare function {fn} failed: {e}")
            continue

    # All functions failed
    raise AkAdapterError(f"All AkShare functions failed: {ak_functions}")