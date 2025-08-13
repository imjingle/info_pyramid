from __future__ import annotations

import importlib
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


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


def _call_single(ak_module, fn_name: str, params: Dict[str, Any]) -> pd.DataFrame:
    fn = getattr(ak_module, fn_name, None)
    if not callable(fn):
        raise AkAdapterError(f"AkShare function not found: {fn_name}")
    call_params = {k: v for k, v in params.items() if v is not None}
    data = fn(**call_params) if call_params else fn()
    return data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)


def call_akshare(
    ak_functions: List[str],
    params: Dict[str, Any],
    field_mapping: Optional[Dict[str, str]] = None,
    allow_fallback: bool = False,
    function_name: Optional[str] = None,
) -> Tuple[str, pd.DataFrame]:
    ak = _import_akshare()

    if function_name:
        df = _call_single(ak, function_name, params)
        df = _rename_columns(df, field_mapping)
        df = _ensure_symbol_column(df, params)
        df = _normalize_types(df)
        return function_name, df

    if not allow_fallback:
        if len(ak_functions) == 1:
            fn = ak_functions[0]
            df = _call_single(ak, fn, params)
            df = _rename_columns(df, field_mapping)
            df = _ensure_symbol_column(df, params)
            df = _normalize_types(df)
            return fn, df
        raise AkAdapterError(
            f"Multiple candidate functions available, but no explicit choice provided: {ak_functions}"
        )

    last_err: Optional[Exception] = None
    for name in ak_functions:
        try:
            df = _call_single(ak, name, params)
            df = _rename_columns(df, field_mapping)
            df = _ensure_symbol_column(df, params)
            df = _normalize_types(df)
            if df.empty:
                last_err = AkAdapterError(f"Function {name} returned empty result")
                continue
            return name, df
        except Exception as exc:
            last_err = exc
            continue
    raise AkAdapterError(
        f"All candidate AkShare functions failed: {ak_functions}. Last error: {last_err}"
    )