from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, List

from fastapi import FastAPI, Query, Body
from sse_starlette.sse import EventSourceResponse

from .dispatcher import fetch_data, get_ohlcv, get_market_quote
from .registry import REGISTRY
from .adapters.qmt_adapter import test_qmt_import  # type: ignore

app = FastAPI(title="AK Unified API", version="0.1.0")


def _apply_adapter_variant(dataset_id: str, adapter: Optional[str]) -> str:
    if not adapter:
        return dataset_id
    candidate = f"{dataset_id}.{adapter}"
    if candidate in REGISTRY:
        return candidate
    return dataset_id


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
            "platform": getattr(spec, 'platform', 'cross'),
            "notes": getattr(spec, 'notes', None),
        })
    return {"items": items}


@app.get("/rpc/fetch")
async def rpc_fetch(
    dataset_id: str = Query(...),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
    adapter: Optional[str] = Query(None),
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
    dataset_id = _apply_adapter_variant(dataset_id, adapter)
    env = fetch_data(dataset_id, params, ak_function=ak_function, allow_fallback=allow_fallback)
    return env.model_dump()


@app.get("/rpc/fetch_async")
async def rpc_fetch_async(
    dataset_id: str = Query(...),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
    adapter: Optional[str] = Query(None),
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {k: v for k, v in {"symbol": symbol, "start": start, "end": end}.items() if v is not None}
    dataset_id = _apply_adapter_variant(dataset_id, adapter)
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
    loop = asyncio.get_running_loop()
    env = await loop.run_in_executor(None, lambda: fetch_data(dataset_id, params, ak_function=ak_function, allow_fallback=allow_fallback))
    return env.model_dump()


@app.post("/rpc/batch")
async def rpc_batch(
    tasks: List[Dict[str, Any]] = Body(..., embed=False)
) -> List[Dict[str, Any]]:
    async def run_task(task: Dict[str, Any]) -> Dict[str, Any]:
        ds = _apply_adapter_variant(task.get('dataset_id'), task.get('adapter'))
        params = task.get('params') or {}
        ak_function = task.get('ak_function')
        allow_fallback = bool(task.get('allow_fallback', False))
        spec = REGISTRY.get(ds)
        try:
            if spec and getattr(spec, 'adapter', 'akshare') == 'baostock':
                from .adapters.baostock_adapter import acall_baostock
                tag, df = await acall_baostock(ds, params)
                return {
                    "dataset": ds,
                    "ok": True,
                    "ak_function": tag,
                    "data_source": "baostock",
                    "data": df.to_dict(orient='records'),
                }
            loop = asyncio.get_running_loop()
            env = await loop.run_in_executor(None, lambda: fetch_data(ds, params, ak_function=ak_function, allow_fallback=allow_fallback))
            return {"dataset": ds, "ok": True, "ak_function": env.ak_function, "data_source": env.data_source, "data": env.data}
        except Exception as e:  # noqa: BLE001
            return {"dataset": ds, "ok": False, "error": str(e)}

    results = await asyncio.gather(*(run_task(t) for t in tasks), return_exceptions=False)
    return results


@app.get("/rpc/ohlcv")
async def rpc_ohlcv(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    adjust: str = "none",
    ak_function: Optional[str] = None,
    allow_fallback: bool = False,
    adapter: Optional[str] = None,
) -> Dict[str, Any]:
    # ohlcv uses the CN akshare dataset; for other adapters use /rpc/fetch with adapter
    env = get_ohlcv(symbol, start=start, end=end, adjust=adjust, ak_function=ak_function, allow_fallback=allow_fallback)
    return env.model_dump()


async def _polling_generator(dataset_id: str, params: Dict[str, Any], ak_function: Optional[str], adapter: Optional[str], interval_sec: float, symbols: Optional[List[str]] = None):
    dataset_id = _apply_adapter_variant(dataset_id, adapter)
    # QMT subscription lifecycle for realtime quotes
    if adapter == 'qmt' and dataset_id.endswith('quote') and symbols:
        try:
            from .adapters.qmt_adapter import subscribe_quotes, unsubscribe_quotes, fetch_realtime_quotes  # type: ignore
            subscribe_quotes(symbols)
            while True:
                tag, df = fetch_realtime_quotes(symbols)
                payload = {
                    "schema_version": "1.0.0",
                    "provider": "qmt",
                    "dataset": dataset_id,
                    "params": params,
                    "data": df.to_dict(orient="records"),
                    "ak_function": tag,
                }
                yield {"event": "update", "data": payload}
                await asyncio.sleep(interval_sec)
        finally:
            try:
                unsubscribe_quotes(symbols)
            except Exception:
                pass
    # generic path
    while True:
        spec = REGISTRY.get(dataset_id)
        if spec and getattr(spec, 'adapter', 'akshare') == 'baostock':
            from .adapters.baostock_adapter import acall_baostock
            tag, df = await acall_baostock(dataset_id, params)
            payload = {
                "schema_version": "1.0.0",
                "provider": "baostock",
                "dataset": dataset_id,
                "params": params,
                "data": df.to_dict(orient="records"),
                "ak_function": tag,
            }
            yield {"event": "update", "data": payload}
        else:
            env = fetch_data(dataset_id, params, ak_function=ak_function, allow_fallback=False)
            yield {"event": "update", "data": env.model_dump()}
        await asyncio.sleep(interval_sec)


@app.get("/topic/stream")
async def topic_stream(
    dataset_id: str,
    ak_function: Optional[str] = None,
    interval: float = 2.0,
    adapter: Optional[str] = None,
    # common params accepted for streaming
    symbol: Optional[str] = None,
    symbols: Optional[List[str]] = Query(None),
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
    sym_list = symbols or ([symbol] if symbol else None)
    generator = _polling_generator(dataset_id, params, ak_function, adapter, interval, sym_list)
    return EventSourceResponse(generator)


@app.get("/rpc/quote")
async def rpc_quote(ak_function: Optional[str] = None, allow_fallback: bool = False, adapter: Optional[str] = None, symbols: Optional[List[str]] = Query(None)) -> Dict[str, Any]:
    if adapter == 'qmt':
        try:
            from .adapters.qmt_adapter import fetch_realtime_quotes  # type: ignore
            tag, df = fetch_realtime_quotes(symbols)
            return {"schema_version": "1.0.0", "provider": "qmt", "dataset": "securities.equity.cn.quote.qmt", "params": {"symbols": symbols}, "data": df.to_dict(orient='records'), "ak_function": tag, "data_source": "qmt"}
        except Exception as e:  # noqa: BLE001
            return {"schema_version": "1.0.0", "provider": "qmt", "error": str(e)}
    env = get_market_quote(ak_function=ak_function, allow_fallback=allow_fallback)
    return env.model_dump()


@app.get("/providers/qmt/status")
async def qmt_status() -> Dict[str, Any]:
    return test_qmt_import()


@app.post("/providers/qmt/subscribe")
async def qmt_subscribe(symbols: List[str] = Body(...)) -> Dict[str, Any]:
    try:
        from .adapters.qmt_adapter import subscribe_quotes  # type: ignore
        tag = subscribe_quotes(symbols)
        return {"ok": True, "ak_function": tag}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}


@app.post("/providers/qmt/unsubscribe")
async def qmt_unsubscribe(symbols: List[str] = Body(...)) -> Dict[str, Any]:
    try:
        from .adapters.qmt_adapter import unsubscribe_quotes  # type: ignore
        tag = unsubscribe_quotes(symbols)
        return {"ok": True, "ak_function": tag}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}


@app.get("/providers/qmt/quotes")
async def qmt_quotes(symbols: Optional[List[str]] = Query(None)) -> Dict[str, Any]:
    try:
        from .adapters.qmt_adapter import fetch_realtime_quotes  # type: ignore
        tag, df = fetch_realtime_quotes(symbols)
        return {"ok": True, "ak_function": tag, "data": df.to_dict(orient='records')}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}