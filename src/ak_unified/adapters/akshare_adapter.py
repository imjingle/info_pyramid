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


def _find_first_available_function(ak_module, candidate_names: List[str]):
    for name in candidate_names:
        fn = getattr(ak_module, name, None)
        if callable(fn):
            return name, fn
    raise AkAdapterError(f"None of candidate AkShare functions found: {candidate_names}")


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
    # Best-effort numeric conversion
    for c in df.columns:
        if c in {"date", "datetime"}:
            continue
        df[c] = pd.to_numeric(df[c], errors="ignore")
    return df


def call_akshare(
    ak_functions: List[str],
    params: Dict[str, Any],
    field_mapping: Optional[Dict[str, str]] = None,
) -> Tuple[str, pd.DataFrame]:
    ak = _import_akshare()
    fn_name, fn = _find_first_available_function(ak, ak_functions)

    # Filter out None values to avoid unexpected AkShare param errors
    call_params = {k: v for k, v in params.items() if v is not None}

    data = fn(**call_params) if call_params else fn()
    if isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        # Many akshare APIs return DataFrame; if not, try to convert
        df = pd.DataFrame(data)

    df = _rename_columns(df, field_mapping)
    df = _ensure_symbol_column(df, params)
    df = _normalize_types(df)
    return fn_name, df