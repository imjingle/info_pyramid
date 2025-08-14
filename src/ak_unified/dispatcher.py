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


def fetch_data(dataset_id: str, params: Optional[Dict[str, Any]] = None, *, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    spec = _resolve_spec(dataset_id)
    params = params or {}

    # computed dataset
    if spec.compute is not None:
        df = spec.compute(params)
        records = df.to_dict(orient="records")
        env = _envelope(spec, params, records)
        env.ak_function = "computed"
        env.data_source = spec.source or "computed"
        return env

    ak_params = _apply_param_transform(spec, params)

    # dispatch adapter
    if spec.adapter == "akshare":
        fn_used, df = call_akshare(
            spec.ak_functions,
            ak_params,
            field_mapping=spec.field_mapping,
            allow_fallback=allow_fallback,
            function_name=ak_function,
        )
    elif spec.adapter == "baostock":
        from .adapters.baostock_adapter import call_baostock
        fn_used, df = call_baostock(dataset_id, ak_params)
    elif spec.adapter == "mootdx":
        from .adapters.mootdx_adapter import call_mootdx
        fn_used, df = call_mootdx(dataset_id, ak_params)
    elif spec.adapter == "qmt":
        from .adapters.qmt_adapter import call_qmt
        fn_used, df = call_qmt(dataset_id, ak_params)
    elif spec.adapter == "efinance":
        from .adapters.efinance_adapter import call_efinance
        fn_used, df = call_efinance(dataset_id, ak_params)
    elif spec.adapter == "qstock":
        from .adapters.qstock_adapter import call_qstock
        fn_used, df = call_qstock(dataset_id, ak_params)
    elif spec.adapter == "adata":
        from .adapters.adata_adapter import call_adata
        fn_used, df = call_adata(dataset_id, ak_params)
    else:
        raise RuntimeError(f"Unknown adapter: {spec.adapter}")

    df = _postprocess(spec, df, params)
    records = df.to_dict(orient="records")
    env = _envelope(spec, params, records)
    env.ak_function = fn_used
    env.data_source = spec.source
    return env


# ------------- Convenience APIs -------------

def get_ohlcv(symbol: str, start: Optional[str] = None, end: Optional[str] = None, adjust: str = "none", *, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    params = {"symbol": symbol, "start": start, "end": end, "adjust": adjust}
    return fetch_data("securities.equity.cn.ohlcv_daily", params, ak_function=ak_function, allow_fallback=allow_fallback)


def get_market_quote(*, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    return fetch_data("securities.equity.cn.quote", {}, ak_function=ak_function, allow_fallback=allow_fallback)


def get_index_constituents(index_code: str, *, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    return fetch_data("market.index.constituents", {"index_code": index_code}, ak_function=ak_function, allow_fallback=allow_fallback)


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
    ak_function = kwargs.pop("ak_function", None)
    allow_fallback = kwargs.pop("allow_fallback", False)
    return fetch_data(dataset, kwargs, ak_function=ak_function, allow_fallback=allow_fallback)


def get_fund_nav(fund_code: str, start: Optional[str] = None, end: Optional[str] = None, *, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    is_etf_like = fund_code.isdigit() and fund_code.startswith(("5", "1"))
    dataset = "securities.fund.cn.nav" if is_etf_like else "securities.fund.cn.nav_open"
    params: Dict[str, Any] = {"fund_code": fund_code}
    if dataset == "securities.fund.cn.nav":
        params.update({"start": start, "end": end})
    return fetch_data(dataset, params, ak_function=ak_function, allow_fallback=allow_fallback)