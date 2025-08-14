from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query
from sse_starlette.sse import EventSourceResponse

from .dispatcher import fetch_data, get_ohlcv, get_market_quote
from .registry import REGISTRY

app = FastAPI(title="AK Unified API", version="0.1.0")


@app.get("/rpc/datasets")
async def rpc_datasets() -> Dict[str, Any]:
    items = []
    for ds_id, spec in REGISTRY.items():
        items.append({
            "dataset_id": ds_id,
            "category": spec.category,
            "domain": spec.domain,
            "ak_functions": spec.ak_functions,
            "source": spec.source,
            "computed": spec.compute is not None,
            "adapter": getattr(spec, 'adapter', 'akshare'),
        })
    return {"items": items}


@app.get("/rpc/fetch")
async def rpc_fetch(
    dataset_id: str = Query(...),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
    # common params (optional)
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    date: Optional[str] = None,
    freq: Optional[str] = None,
    adjust: Optional[str] = None,
    board_code: Optional[str] = None,
    index_code: Optional[str] = None,
    market: Optional[str] = None,
    series: Optional[str] = None,
    segment: Optional[str] = None,
    indicator: Optional[str] = None,
    fund_code: Optional[str] = None,
    symbol2: Optional[str] = None,
    window_days: Optional[int] = None,
    min_periods: Optional[int] = None,
    pmi_threshold: Optional[float] = None,
    growth_high: Optional[float] = None,
    growth_low: Optional[float] = None,
    pmi_recession: Optional[float] = None,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    for k, v in {
        "symbol": symbol,
        "start": start,
        "end": end,
        "date": date,
        "freq": freq,
        "adjust": adjust,
        "board_code": board_code,
        "index_code": index_code,
        "market": market,
        "series": series,
        "segment": segment,
        "indicator": indicator,
        "fund_code": fund_code,
        "symbol2": symbol2,
        "window_days": window_days,
        "min_periods": min_periods,
        "pmi_threshold": pmi_threshold,
        "growth_high": growth_high,
        "growth_low": growth_low,
        "pmi_recession": pmi_recession,
    }.items():
        if v is not None:
            params[k] = v
    env = fetch_data(dataset_id, params, ak_function=ak_function, allow_fallback=allow_fallback)
    return env.model_dump()


@app.get("/rpc/fetch_async")
async def rpc_fetch_async(
    dataset_id: str = Query(...),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Dict[str, Any]:
    # limited async path for baostock datasets
    params: Dict[str, Any] = {k: v for k, v in {"symbol": symbol, "start": start, "end": end}.items() if v is not None}
    spec = REGISTRY.get(dataset_id)
    if spec and getattr(spec, 'adapter', 'akshare') == 'baostock':
        from .adapters.baostock_adapter import acall_baostock
        tag, df = await acall_baostock(dataset_id, params)
        env = {
            "schema_version": "1.0.0",
            "provider": "baostock",
            "category": spec.category,
            "domain": spec.domain,
            "dataset": dataset_id,
            "params": params,
            "timezone": "Asia/Shanghai",
            "currency": None,
            "attribution": "Data via BaoStock",
            "data": df.to_dict(orient="records"),
            "pagination": {"offset": 0, "limit": len(df), "total": len(df)},
            "ak_function": tag,
            "data_source": "baostock",
        }
        return env
    # fallback to sync
    env = fetch_data(dataset_id, params, ak_function=ak_function, allow_fallback=allow_fallback)
    return env.model_dump()


@app.get("/rpc/ohlcv")
async def rpc_ohlcv(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    adjust: str = "none",
    ak_function: Optional[str] = None,
    allow_fallback: bool = False,
) -> Dict[str, Any]:
    env = get_ohlcv(symbol, start=start, end=end, adjust=adjust, ak_function=ak_function, allow_fallback=allow_fallback)
    return env.model_dump()


@app.get("/rpc/quote")
async def rpc_quote(ak_function: Optional[str] = None, allow_fallback: bool = False) -> Dict[str, Any]:
    env = get_market_quote(ak_function=ak_function, allow_fallback=allow_fallback)
    return env.model_dump()


async def _polling_generator(dataset_id: str, params: Dict[str, Any], ak_function: Optional[str], interval_sec: float):
    while True:
        env = fetch_data(dataset_id, params, ak_function=ak_function, allow_fallback=False)
        yield {
            "event": "update",
            "data": env.model_dump_json()
        }
        await asyncio.sleep(interval_sec)


@app.get("/topic/stream")
async def topic_stream(
    dataset_id: str,
    ak_function: Optional[str] = None,
    interval: float = 2.0,
    # common params accepted for streaming
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    date: Optional[str] = None,
    freq: Optional[str] = None,
    adjust: Optional[str] = None,
    board_code: Optional[str] = None,
    index_code: Optional[str] = None,
    market: Optional[str] = None,
    series: Optional[str] = None,
    segment: Optional[str] = None,
    indicator: Optional[str] = None,
    fund_code: Optional[str] = None,
    symbol2: Optional[str] = None,
    window_days: Optional[int] = None,
    min_periods: Optional[int] = None,
) -> EventSourceResponse:
    params: Dict[str, Any] = {}
    for k, v in {
        "symbol": symbol,
        "start": start,
        "end": end,
        "date": date,
        "freq": freq,
        "adjust": adjust,
        "board_code": board_code,
        "index_code": index_code,
        "market": market,
        "series": series,
        "segment": segment,
        "indicator": indicator,
        "fund_code": fund_code,
        "symbol2": symbol2,
        "window_days": window_days,
        "min_periods": min_periods,
    }.items():
        if v is not None:
            params[k] = v
    generator = _polling_generator(dataset_id, params, ak_function, interval)
    return EventSourceResponse(generator)