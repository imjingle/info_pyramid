from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pandas as _pd

from .schemas.envelope import DataEnvelope, Pagination
from .registry import REGISTRY, DatasetSpec
from .adapters.akshare_adapter import call_akshare
from .storage import get_pool as _get_pool, fetch_records as _db_fetch, upsert_records as _db_upsert, upsert_blob_snapshot as _db_upsert_blob, fetch_blob_snapshot as _db_fetch_blob
from .storage import fetch_blob_range as _db_fetch_blob_range
import asyncio
from .normalization import apply_and_validate
from .logging import logger


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


def _get_time_field(spec: DatasetSpec) -> str:
    """Get the time field for the dataset."""
    if "ohlcv_min" in spec.dataset_id or spec.dataset_id.endswith(".ohlcva_min"):
        return "datetime"
    return "date"


def _is_realtime(params: Dict[str, Any]) -> bool:
    """Check if the request is for realtime data."""
    return 'quote' in str(params.get('dataset_id', ''))


async def fetch_data(
    dataset_id: str,
    params: Dict[str, Any],
    allow_fallback: bool = False,
    ak_function: Optional[str] = None,
    adapter: Optional[str] = None,
    use_cache: bool = True,
    use_blob: bool = True,
    store_blob: bool = True,
) -> DataEnvelope:
    """Fetch data from upstream source with caching."""
    spec = REGISTRY.get(dataset_id)
    if not spec:
        raise ValueError(f"Unknown dataset: {dataset_id}")

    # Override adapter if specified
    if adapter:
        spec = spec.with_adapter(adapter)

    # Apply parameter transformations
    ak_params = _apply_param_transform(spec, params)
    time_field = _get_time_field(spec)
    is_realtime = _is_realtime(params)

    # Try cache first
    cached: List[Dict[str, Any]] = []
    if use_cache and not is_realtime:
        try:
            pool = await _get_pool()
            if pool:
                cached = await _db_fetch(pool, dataset_id, params)
        except Exception:
            pass

    # Fetch from upstream if needed
    new_records: List[Dict[str, Any]] = []
    if not cached or is_realtime:
        if spec.adapter == "akshare":
            fn_used, df = await call_akshare(
                spec.ak_functions,
                ak_params,
                field_mapping=spec.field_mapping,
                allow_fallback=allow_fallback,
                function_name=ak_function,
            )
        elif spec.adapter == "baostock":
            from .adapters.baostock_adapter import call_baostock
            fn_used, df = await asyncio.to_thread(call_baostock, dataset_id, ak_params)
        elif spec.adapter == "mootdx":
            from .adapters.mootdx_adapter import call_mootdx
            fn_used, df = await asyncio.to_thread(call_mootdx, dataset_id, ak_params)
        elif spec.adapter == "qmt":
            from .adapters.qmt_adapter import call_qmt
            fn_used, df = await call_qmt(dataset_id, ak_params)
        elif spec.adapter == "efinance":
            from .adapters.efinance_adapter import call_efinance
            fn_used, df = await asyncio.to_thread(call_efinance, dataset_id, ak_params)
        elif spec.adapter == "qstock":
            from .adapters.qstock_adapter import call_qstock
            fn_used, df = await asyncio.to_thread(call_qstock, dataset_id, ak_params)
        elif spec.adapter == "adata":
            from .adapters.adata_adapter import call_adata
            fn_used, df = await asyncio.to_thread(call_adata, dataset_id, ak_params)
        elif spec.adapter == "yfinance":
            from .adapters.yfinance_adapter import call_yfinance
            fn_used, df = await asyncio.to_thread(call_yfinance, dataset_id, ak_params)
        elif spec.adapter == "alphavantage":
            from .adapters.alphavantage_adapter import call_alphavantage
            fn_used, df = await call_alphavantage(dataset_id, ak_params)
        elif spec.adapter == "ibkr":
            from .adapters.ibkr_adapter import call_ibkr
            fn_used, df = await asyncio.to_thread(call_ibkr, dataset_id, ak_params)
        else:
            raise RuntimeError(f"Unknown adapter: {spec.adapter}")
        logger.bind(dataset=dataset_id, adapter=spec.adapter, fn=fn_used).info("fetched upstream span")
        df = _postprocess(spec, df, ak_params)
        new_records.extend(df.to_dict(orient="records"))

    records = cached + new_records

    # upsert
    if new_records:
        try:
            pool = await _get_pool()
            if pool:
                await _db_upsert(pool, dataset_id, new_records)
        except Exception:
            pass

    env = _envelope(spec, params, records)
    env.ak_function = 'cache+source'
    env.data_source = spec.source
    return env


async def fetch_data_batch(
    tasks: List[Tuple[str, Dict[str, Any]]],
    max_concurrent: int = 5,
    allow_fallback: bool = False,
    use_cache: bool = True,
    use_blob: bool = True,
    store_blob: bool = True,
) -> List[DataEnvelope]:
    """
    Fetch multiple datasets concurrently with rate limiting and concurrency control.
    
    Args:
        tasks: List of (dataset_id, params) tuples
        max_concurrent: Maximum concurrent requests
        allow_fallback: Whether to allow fallback for AkShare
        use_cache: Whether to use cache
        use_blob: Whether to use blob storage
        store_blob: Whether to store blob data
        
    Returns:
        List of DataEnvelope objects in the same order as tasks
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def fetch_with_semaphore(task: Tuple[str, Dict[str, Any]]) -> DataEnvelope:
        async with semaphore:
            dataset_id, params = task
            return await fetch_data(
                dataset_id=dataset_id,
                params=params,
                allow_fallback=allow_fallback,
                use_cache=use_cache,
                use_blob=use_blob,
                store_blob=store_blob
            )
    
    # Execute all tasks concurrently
    results = await asyncio.gather(
        *[fetch_with_semaphore(task) for task in tasks],
        return_exceptions=True
    )
    
    # Handle exceptions and maintain order
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Task {i} failed: {result}")
            # Create an error envelope
            error_env = DataEnvelope(
                category="error",
                domain="error",
                dataset=tasks[i][0],
                params=tasks[i][1],
                timezone=DEFAULT_TIMEZONE,
                currency=None,
                data=[],
                pagination=Pagination(offset=0, limit=0, total=0),
            )
            error_env.ak_function = 'error'
            error_env.data_source = 'error'
            processed_results.append(error_env)
        else:
            processed_results.append(result)
    
    return processed_results


async def fetch_data_with_rate_limiting(
    dataset_id: str,
    params: Dict[str, Any],
    rate_limit_source: Optional[str] = None,
    **kwargs: Any
) -> DataEnvelope:
    """
    Fetch data with explicit rate limiting for a specific source.
    
    Args:
        dataset_id: Dataset identifier
        params: Query parameters
        rate_limit_source: Source name for rate limiting (e.g., 'alphavantage', 'akshare')
        **kwargs: Additional arguments for fetch_data
        
    Returns:
        DataEnvelope with fetched data
    """
    if rate_limit_source:
        from .rate_limiter import acquire_rate_limit, acquire_daily_rate_limit
        await acquire_rate_limit(rate_limit_source)
        await acquire_daily_rate_limit(rate_limit_source)
    
    return await fetch_data(dataset_id, params, **kwargs)


# ------------- Convenience APIs -------------

async def get_ohlcv(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    adjust: str = "none",
    *,
    ak_function: Optional[str] = None,
    allow_fallback: bool = False,
) -> DataEnvelope:
    params = {"symbol": symbol, "start": start, "end": end, "adjust": adjust}
    return await fetch_data("securities.equity.cn.ohlcv_daily", params, ak_function=ak_function, allow_fallback=allow_fallback)


async def get_market_quote(*, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    return await fetch_data("securities.equity.cn.quote", {}, ak_function=ak_function, allow_fallback=allow_fallback)


async def get_index_constituents(index_code: str, *, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    return await fetch_data("market.index.constituents", {"index_code": index_code}, ak_function=ak_function, allow_fallback=allow_fallback)


async def get_macro_indicator(region: str, indicator_id: str, **kwargs: Any) -> DataEnvelope:
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
    return await fetch_data(dataset, kwargs, ak_function=ak_function, allow_fallback=allow_fallback)


async def get_fund_nav(
    fund_code: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    *,
    ak_function: Optional[str] = None,
    allow_fallback: bool = False,
) -> DataEnvelope:
    is_etf_like = fund_code.isdigit() and fund_code.startswith(("5", "1"))
    dataset = "securities.fund.cn.nav" if is_etf_like else "securities.fund.cn.nav_open"
    params: Dict[str, Any] = {"fund_code": fund_code}
    if dataset == "securities.fund.cn.nav":
        params.update({"start": start, "end": end})
    return await fetch_data(dataset, params, ak_function=ak_function, allow_fallback=allow_fallback)


async def get_ohlcva(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    adjust: str = "none",
    *,
    ak_function: Optional[str] = None,
    allow_fallback: bool = False,
) -> DataEnvelope:
    params = {"symbol": symbol, "start": start, "end": end, "adjust": adjust}
    return await fetch_data("securities.equity.cn.ohlcva_daily", params, ak_function=ak_function, allow_fallback=allow_fallback)


# ------------- Batch Convenience APIs -------------

async def get_ohlcv_batch(
    symbols: List[str],
    start: Optional[str] = None,
    end: Optional[str] = None,
    adjust: str = "none",
    max_concurrent: int = 5,
    *,
    ak_function: Optional[str] = None,
    allow_fallback: bool = False,
) -> List[DataEnvelope]:
    """Get OHLCV data for multiple symbols concurrently."""
    tasks = [
        ("securities.equity.cn.ohlcv_daily", {
            "symbol": symbol, "start": start, "end": end, "adjust": adjust
        })
        for symbol in symbols
    ]
    return await fetch_data_batch(
        tasks,
        max_concurrent=max_concurrent,
        allow_fallback=allow_fallback
    )


async def get_market_quotes_batch(
    symbols: List[str],
    max_concurrent: int = 5,
    *,
    ak_function: Optional[str] = None,
    allow_fallback: bool = False,
) -> List[DataEnvelope]:
    """Get market quotes for multiple symbols concurrently."""
    tasks = [
        ("securities.equity.cn.quote", {"symbol": symbol})
        for symbol in symbols
    ]
    return await fetch_data_batch(
        tasks,
        max_concurrent=max_concurrent,
        allow_fallback=allow_fallback
    )


async def get_index_constituents_batch(
    index_codes: List[str],
    max_concurrent: int = 5,
    *,
    ak_function: Optional[str] = None,
    allow_fallback: bool = False,
) -> List[DataEnvelope]:
    """Get index constituents for multiple indices concurrently."""
    tasks = [
        ("market.index.constituents", {"index_code": index_code})
        for index_code in index_codes
    ]
    return await fetch_data_batch(
        tasks,
        max_concurrent=max_concurrent,
        allow_fallback=allow_fallback
    )