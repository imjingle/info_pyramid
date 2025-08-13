from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from .schemas.envelope import DataEnvelope, Pagination
from .registry import REGISTRY, DatasetSpec
from .adapters.akshare_adapter import call_akshare


DEFAULT_TIMEZONE = "Asia/Shanghai"


def _resolve_spec(dataset_id: str) -> DatasetSpec:
    if dataset_id not in REGISTRY:
        raise KeyError(f"Dataset not registered: {dataset_id}")
    return REGISTRY[dataset_id]


def _apply_param_transform(spec: DatasetSpec, params: Dict[str, Any]) -> Dict[str, Any]:
    if spec.param_transform is None:
        return params
    return spec.param_transform(params)


def _postprocess(spec: DatasetSpec, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    if spec.postprocess is None:
        return df
    return spec.postprocess(df, params)


def _envelope(
    spec: DatasetSpec, params: Dict[str, Any], data: List[Dict[str, Any]], currency: Optional[str] = None
) -> DataEnvelope:
    return DataEnvelope(
        category=spec.category,
        domain=spec.domain,
        dataset=spec.dataset_id,
        params=params,
        timezone=DEFAULT_TIMEZONE,
        currency=currency,
        data=data,
        pagination=Pagination(offset=0, limit=len(data), total=len(data)),
    )


def fetch_data(dataset_id: str, params: Optional[Dict[str, Any]] = None) -> DataEnvelope:
    spec = _resolve_spec(dataset_id)
    params = params or {}
    ak_params = _apply_param_transform(spec, params)
    _, df = call_akshare(spec.ak_functions, ak_params, field_mapping=spec.field_mapping)
    df = _postprocess(spec, df, params)
    records = df.to_dict(orient="records")
    return _envelope(spec, params, records)


# ------------- Convenience APIs -------------

def get_ohlcv(symbol: str, start: Optional[str] = None, end: Optional[str] = None, adjust: str = "none") -> DataEnvelope:
    params = {"symbol": symbol, "start": start, "end": end, "adjust": adjust}
    return fetch_data("securities.equity.cn.ohlcv_daily", params)


def get_market_quote() -> DataEnvelope:
    return fetch_data("securities.equity.cn.quote", {})


def get_index_constituents(index_code: str) -> DataEnvelope:
    return fetch_data("market.index.constituents", {"index_code": index_code})


def get_macro_indicator(region: str, indicator_id: str, **kwargs: Any) -> DataEnvelope:
    key = (region.upper(), indicator_id.lower())
    mapping = {
        ("CN", "ppi"): "macro.cn.ppi",
        ("CN", "pmi"): "macro.cn.pmi",
        ("CN", "gdp"): "macro.cn.gdp",
    }
    dataset = mapping.get(key)
    if not dataset:
        raise KeyError(f"Macro indicator not mapped: region={region} indicator_id={indicator_id}")
    return fetch_data(dataset, kwargs)


def get_fund_nav(fund_code: str, start: Optional[str] = None, end: Optional[str] = None) -> DataEnvelope:
    # Heuristic: 6-digit code with leading 5/1 often ETF/LOF; allow caller to pass ETFs directly to ETF dataset
    is_etf_like = fund_code.isdigit() and fund_code.startswith(("5", "1"))
    dataset = "securities.fund.cn.nav" if is_etf_like else "securities.fund.cn.nav_open"
    params: Dict[str, Any] = {"fund_code": fund_code}
    if dataset == "securities.fund.cn.nav":
        params.update({"start": start, "end": end})
    return fetch_data(dataset, params)