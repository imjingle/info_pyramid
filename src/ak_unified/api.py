from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, Optional, List

import pandas as pd
from fastapi import FastAPI, Query, Body
from sse_starlette.sse import EventSourceResponse
from starlette.responses import JSONResponse
from .logging import logger

from .dispatcher import (
    fetch_data, get_ohlcv, get_market_quote, get_ohlcva,
    fetch_data_batch, get_ohlcv_batch, get_market_quotes_batch, get_index_constituents_batch
)
from .registry import REGISTRY
from .schemas.envelope import DataEnvelope, Pagination
from .schemas.events import (
    EarningsEvent, EarningsForecast, EarningsCalendarRequest, EarningsForecastRequest,
    EarningsCalendarResponse, EarningsForecastResponse
)
from .schemas.financial import (
    FinancialIndicator, FinancialDataRequest, FinancialIndicatorsResponse, 
    FinancialStatementResponse, FinancialRatioResponse
)
from .schemas.fund import (
    FundPortfolioRequest, FundHoldingsChangeRequest, FundTopHoldingsRequest,
    FundPortfolioResponse, FundHoldingsChangeResponse, FundTopHoldingsResponse
)
from .schemas.snowball import (
    SnowballQuoteRequest, SnowballFinancialDataRequest, SnowballResearchReportRequest,
    SnowballSentimentRequest, SnowballDiscussionRequest, SnowballMarketOverviewRequest,
    SnowballQuoteResponse, SnowballFinancialDataResponse, SnowballResearchReportResponse,
    SnowballSentimentResponse, SnowballDiscussionResponse, SnowballMarketOverviewResponse
)
from .schemas.easytrader import (
    EasyTraderLoginRequest, EasyTraderTradingHistoryRequest, EasyTraderMarketDataRequest,
    EasyTraderLoginResponse, EasyTraderAccountInfoResponse, EasyTraderPortfolioResponse,
    EasyTraderTradingHistoryResponse, EasyTraderMarketDataResponse, EasyTraderFundInfoResponse,
    EasyTraderRiskMetricsResponse
)
from .adapters.qmt_adapter import test_qmt_import  # type: ignore
from .adapters.earnings_calendar_adapter import call_earnings_calendar
from .adapters.financial_data_adapter import call_financial_data
from .adapters.fund_portfolio_adapter import call_fund_portfolio
from .adapters.snowball_adapter import call_snowball
from .adapters.easytrader_adapter import call_easytrader
from .storage import get_pool as _get_pool, cache_stats as _cache_stats, purge_records as _purge_records  # type: ignore
from .storage import fetch_blob_snapshot as _blob_fetch, upsert_blob_snapshot as _blob_upsert, purge_blob as _blob_purge  # type: ignore
from .normalization import apply_and_validate
from .rate_limiter import get_rate_limit_status

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


@app.get("/rpc/rate-limits")
async def rpc_rate_limits() -> Dict[str, Any]:
    """Get current rate limiter status for all data sources."""
    try:
        status = await get_rate_limit_status()
        return {
            "rate_limiting_enabled": True,
            "limiters": status
        }
    except Exception as e:
        logger.error(f"Failed to get rate limit status: {e}")
        return {
            "rate_limiting_enabled": False,
            "error": str(e)
        }


@app.get("/rpc/fetch")
async def rpc_fetch(
    dataset_id: str = Query(...),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
    adapter: Optional[str] = Query(None),
    use_cache: bool = Query(True),
    use_blob: bool = Query(True),
    store_blob: bool = Query(True),
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
    env = await fetch_data(dataset_id, params, ak_function=ak_function, allow_fallback=allow_fallback, use_cache=use_cache, use_blob=use_blob, store_blob=store_blob)
    return env.model_dump()


@app.post("/rpc/fetch_batch")
async def rpc_fetch_batch(
    tasks: List[Dict[str, Any]] = Body(..., embed=False),
    max_concurrent: int = Query(5, description="Maximum concurrent requests"),
    allow_fallback: bool = Query(False),
    use_cache: bool = Query(True),
    use_blob: bool = Query(True),
    store_blob: bool = Query(True),
) -> List[Dict[str, Any]]:
    """Fetch multiple datasets concurrently with rate limiting."""
    try:
        # Convert tasks to (dataset_id, params) tuples
        task_tuples = []
        for task in tasks:
            dataset_id = task.get('dataset_id')
            params = task.get('params', {})
            if dataset_id:
                task_tuples.append((dataset_id, params))
        
        if not task_tuples:
            return [{"error": "No valid tasks provided"}]
        
        # Execute batch fetch
        results = await fetch_data_batch(
            task_tuples,
            max_concurrent=max_concurrent,
            allow_fallback=allow_fallback,
            use_cache=use_cache,
            use_blob=use_blob,
            store_blob=store_blob
        )
        
        # Convert to dict format
        return [result.model_dump() for result in results]
        
    except Exception as e:
        logger.error(f"Batch fetch failed: {e}")
        return [{"error": str(e)}]


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
    env = await fetch_data(dataset_id, params, ak_function=ak_function, allow_fallback=allow_fallback)
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
            env = await fetch_data(ds, params, ak_function=ak_function, allow_fallback=allow_fallback)
            return {"dataset": ds, "ok": True, "ak_function": env.ak_function, "data_source": env.data_source, "data": env.data}
        except Exception as e:  # noqa: BLE001
            return {"dataset": ds, "ok": False, "error": str(e)}

    results = await asyncio.gather(*(run_task(t) for t in tasks), return_exceptions=False)
    return results


@app.get("/rpc/ohlcv")
async def rpc_ohlcv(
    symbol: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    adjust: str = Query("none"),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
):
    # ohlcv uses the CN akshare dataset; for other adapters use /rpc/fetch with adapter
    env = await get_ohlcv(symbol, start=start, end=end, adjust=adjust, ak_function=ak_function, allow_fallback=allow_fallback)
    return JSONResponse(content=env.model_dump(mode="json"), media_type="application/json")


@app.get("/rpc/ohlcva")
async def rpc_ohlcva(
    symbol: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    adjust: str = Query("none"),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
):
    env = await get_ohlcva(symbol, start=start, end=end, adjust=adjust, ak_function=ak_function, allow_fallback=allow_fallback)
    return JSONResponse(content=env.model_dump(mode="json"), media_type="application/json")


@app.post("/rpc/ohlcv_batch")
async def rpc_ohlcv_batch(
    symbols: List[str] = Body(..., embed=False),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    adjust: str = Query("none"),
    max_concurrent: int = Query(5, description="Maximum concurrent requests"),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
):
    """Get OHLCV data for multiple symbols concurrently."""
    results = await get_ohlcv_batch(
        symbols=symbols,
        start=start,
        end=end,
        adjust=adjust,
        max_concurrent=max_concurrent,
        ak_function=ak_function,
        allow_fallback=allow_fallback
    )
    return JSONResponse(content=[result.model_dump(mode="json") for result in results], media_type="application/json")


@app.post("/rpc/market_quotes_batch")
async def rpc_market_quotes_batch(
    symbols: List[str] = Body(..., embed=False),
    max_concurrent: int = Query(5, description="Maximum concurrent requests"),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
):
    """Get market quotes for multiple symbols concurrently."""
    results = await get_market_quotes_batch(
        symbols=symbols,
        max_concurrent=max_concurrent,
        ak_function=ak_function,
        allow_fallback=allow_fallback
    )
    return JSONResponse(content=[result.model_dump(mode="json") for result in results], media_type="application/json")


@app.post("/rpc/index_constituents_batch")
async def rpc_index_constituents_batch(
    index_codes: List[str] = Body(..., embed=False),
    max_concurrent: int = Query(5, description="Maximum concurrent requests"),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
):
    """Get index constituents for multiple indices concurrently."""
    results = await get_index_constituents_batch(
        index_codes=index_codes,
        max_concurrent=max_concurrent,
        ak_function=ak_function,
        allow_fallback=allow_fallback
    )
    return JSONResponse(content=[result.model_dump(mode="json") for result in results], media_type="application/json")


@app.get("/rpc/agg/board_snapshot")
async def rpc_board_snapshot(
    board_kind: str = Query("industry"),
    boards: List[str] = Query(...),
    topn: int = Query(5),
    adapter_priority: Optional[List[str]] = Query(None),
    weight_by: str = Query("none"),
):
    params: Dict[str, Any] = {"board_kind": board_kind, "boards": boards, "topn": topn, "weight_by": weight_by}
    if adapter_priority:
        params["adapter_priority"] = adapter_priority
    env = await fetch_data("market.cn.board_aggregation.snapshot", params)
    logger.info("rpc_board_snapshot served")
    return JSONResponse(content=env.model_dump(mode="json"), media_type="application/json")

@app.get("/rpc/agg/index_snapshot")
async def rpc_index_snapshot(
    index_codes: List[str] = Query(...),
    topn: int = Query(5),
    adapter_priority: Optional[List[str]] = Query(None),
    weight_by: str = Query("none"),
):
    params: Dict[str, Any] = {"index_codes": index_codes, "topn": topn, "weight_by": weight_by}
    if adapter_priority:
        params["adapter_priority"] = adapter_priority
    env = await fetch_data("market.cn.index_aggregation.snapshot", params)
    return JSONResponse(content=env.model_dump(mode="json"), media_type="application/json")

@app.get("/rpc/agg/playback")
async def rpc_agg_playback(
    entity_type: str = Query("board"),
    ids: List[str] = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    freq: Optional[str] = Query(None),
    window_n: int = Query(10),
):
    params: Dict[str, Any] = {"entity_type": entity_type, "ids": ids, "start": start, "end": end, "freq": freq, "window_n": window_n}
    env = await fetch_data("market.cn.aggregation.playback", params)
    return JSONResponse(content=env.model_dump(mode="json"), media_type="application/json")

@app.get("/admin/cache/status")
async def admin_cache_status() -> Dict[str, Any]:
    pool = await _get_pool()
    return {"enabled": pool is not None}

@app.get("/admin/cache/stats")
async def admin_cache_stats() -> Dict[str, Any]:
    pool = await _get_pool()
    if pool is None:
        return {"enabled": False, "total": 0, "top_datasets": []}
    stats = await _cache_stats(pool)
    stats["enabled"] = True
    return stats

@app.post("/admin/cache/purge")
async def admin_cache_purge(
    dataset_id: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    index_symbol: Optional[str] = Query(None),
    board_code: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    time_field: Optional[str] = Query(None),
) -> Dict[str, Any]:
    pool = await _get_pool()
    if pool is None:
        return {"enabled": False, "deleted": 0}
    deleted = await _purge_records(pool, dataset_id, symbol=symbol, index_symbol=index_symbol, board_code=board_code, start=start, end=end, time_field=time_field)
    return {"enabled": True, "deleted": int(deleted)}

@app.get("/admin/cache/blob")
async def admin_cache_blob_get(
    dataset_id: str = Query(...),
    symbol: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
) -> Dict[str, Any]:
    pool = await _get_pool()
    if pool is None:
        return {"enabled": False}
    
    # Build params from individual query parameters
    params = {}
    if symbol:
        params["symbol"] = symbol
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    
    res = await _blob_fetch(pool, dataset_id, params)
    if res is None:
        return {"enabled": True, "found": False}
    raw_obj, meta = res
    return {"enabled": True, "found": True, "raw": raw_obj, "meta": meta}

@app.post("/admin/cache/blob/purge")
async def admin_cache_blob_purge(
    dataset_id: Optional[str] = Query(None),
    dataset_prefix: Optional[str] = Query(None),
    updated_after: Optional[str] = Query(None),
    updated_before: Optional[str] = Query(None),
) -> Dict[str, Any]:
    pool = await _get_pool()
    if pool is None:
        return {"enabled": False, "deleted": 0}
    deleted = await _blob_purge(pool, dataset_id, None, dataset_prefix=dataset_prefix, updated_after=updated_after, updated_before=updated_before)
    return {"enabled": True, "deleted": int(deleted)}

@app.get("/rpc/replay")
async def rpc_replay(
    dataset_id: str = Query(...),
    symbol: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    format: str = Query("raw")
) -> Dict[str, Any]:
    pool = await _get_pool()
    if pool is None:
        return {"ok": False, "error": "cache disabled"}
    
    try:
        # Build params from individual query parameters
        params = {}
        if symbol:
            params["symbol"] = symbol
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        
        # Get blob data
        res = await _blob_fetch(pool, dataset_id, params)
        if res is None:
            return {"ok": False, "error": "blob not found"}
        
        raw_obj, meta = res
        # normalize blob
        blob_records = apply_and_validate(dataset_id, raw_obj if isinstance(raw_obj, list) else [])
        # live fetch (bypass cache/blob to compare against upstream)
        env = await fetch_data(dataset_id, params, use_cache=False, use_blob=False)
        live_records = env.data

        def key_of(r: Dict[str, Any]):
            return (r.get('symbol'), r.get('date') or r.get('datetime'))

        blob_set = {key_of(r) for r in blob_records}
        live_set = {key_of(r) for r in live_records}
        missing = live_set - blob_set
        extra = blob_set - live_set
        common = blob_set & live_set

        return {
            "enabled": True,
            "dataset_id": dataset_id,
            "blob_records": len(blob_records),
            "live_records": len(live_records),
            "missing_in_blob": len(missing),
            "extra_in_blob": len(extra),
            "common": len(common),
            "blob_keys": list(blob_set)[:10],
            "live_keys": list(live_set)[:10],
            "missing_keys": list(missing)[:10],
            "extra_keys": list(extra)[:10],
        }
    except Exception as e:
        return {"enabled": True, "error": str(e)}


@app.get("/rpc/stream")
async def rpc_stream(
    dataset_id: str = Query(...),
    symbol: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    interval_sec: int = Query(60),
    max_updates: Optional[int] = Query(None),
    adapter: Optional[str] = Query(None),
    symbols: Optional[List[str]] = Query(None),
    ak_function: Optional[str] = Query(None),
) -> EventSourceResponse:
    """Stream real-time data updates."""
    async def gen():
        update_count = 0
        try:
            # Build params from individual query parameters
            params = {}
            if symbol:
                params["symbol"] = symbol
            if start:
                params["start"] = start
            if end:
                params["end"] = end
            
            # Check if this is a real-time dataset
            if adapter == 'qmt' and dataset_id.endswith('quote') and symbols:
                try:
                    from .adapters.qmt_adapter import subscribe_quotes, unsubscribe_quotes, fetch_realtime_quotes  # type: ignore
                    await subscribe_quotes(symbols)
                    while True:
                        tag, df = await fetch_realtime_quotes(symbols)
                        payload = {
                            "schema_version": "1.0.0",
                            "provider": "qmt",
                            "dataset": dataset_id,
                            "params": params,
                            "data": df.to_dict(orient='records'),
                            "ak_function": tag,
                            "data_source": "qmt",
                            "timestamp": pd.Timestamp.now().isoformat(),
                        }
                        yield {"event": "update", "data": payload}
                        update_count += 1
                        if max_updates and update_count >= max_updates:
                            break
                        await asyncio.sleep(interval_sec)
                finally:
                    try:
                        await unsubscribe_quotes(symbols)
                    except Exception:
                        pass
            # generic path
            else:
                while True:
                    env = await fetch_data(dataset_id, params, ak_function=ak_function, allow_fallback=False)
                    yield {"event": "update", "data": env.model_dump()}
                    update_count += 1
                    if max_updates and update_count >= max_updates:
                        break
                    await asyncio.sleep(interval_sec)
        except Exception as e:
            yield {"event": "error", "data": {"error": str(e)}}

    return EventSourceResponse(gen())


@app.get("/rpc/quote")
async def rpc_quote(
    symbols: Optional[List[str]] = Query(None),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
    adapter: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """Get real-time quotes."""
    if adapter == 'qmt':
        try:
            from .adapters.qmt_adapter import fetch_realtime_quotes  # type: ignore
            tag, df = await fetch_realtime_quotes(symbols)
            return {"schema_version": "1.0.0", "provider": "qmt", "dataset": "securities.equity.cn.quote.qmt", "params": {"symbols": symbols}, "data": df.to_dict(orient='records'), "ak_function": tag, "data_source": "qmt"}
        except Exception as e:  # noqa: BLE001
            return {"schema_version": "1.0.0", "provider": "qmt", "error": str(e)}
    env = await get_market_quote(ak_function=ak_function, allow_fallback=allow_fallback)
    return env.model_dump()


@app.post("/qmt/subscribe")
async def qmt_subscribe(symbols: List[str] = Body(...)) -> Dict[str, Any]:
    try:
        from .adapters.qmt_adapter import subscribe_quotes  # type: ignore
        tag = await subscribe_quotes(symbols)
        return {"ok": True, "ak_function": tag}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}


@app.post("/qmt/unsubscribe")
async def qmt_unsubscribe(symbols: List[str] = Body(...)) -> Dict[str, Any]:
    try:
        from .adapters.qmt_adapter import unsubscribe_quotes  # type: ignore
        tag = await unsubscribe_quotes(symbols)
        return {"ok": True, "ak_function": tag}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}


@app.get("/qmt/quotes")
async def qmt_quotes(symbols: Optional[List[str]] = Query(None)) -> Dict[str, Any]:
    try:
        from .adapters.qmt_adapter import fetch_realtime_quotes  # type: ignore
        tag, df = await fetch_realtime_quotes(symbols)
        return {"ok": True, "ak_function": tag, "data": df.to_dict(orient='records')}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}


@app.get("/rpc/stream/board")
async def rpc_stream_board(
    board_kind: str = Query("industry"),
    boards: List[str] = Query(...),
    topn: int = Query(5),
    adapter_priority: Optional[List[str]] = Query(None),
    weight_by: str = Query("none"),
    interval_sec: int = Query(60),
    max_updates: Optional[int] = Query(None),
    include_percentiles: bool = True,
) -> EventSourceResponse:
    async def gen():
        status = await test_qmt_import()
        if not status.get("ok") and (not adapter_priority or adapter_priority and adapter_priority[0] == 'qmt'):
            yield {"event": "error", "data": {"ok": False, "error": status.get("error"), "is_windows": status.get("is_windows")}}
            return
        ds = 'securities.board.cn.industry.qmt' if board_kind.lower().startswith('i') else 'securities.board.cn.concept.qmt'
        # allow fallback to akshare if qmt board dataset unsupported
        try:
            env = await fetch_data(ds, {})
            df_list = env.data
        except Exception:
            df_list = []
        if not df_list:
            yield {"event": "error", "data": {"ok": False, "error": "No board data available"}}
            return
        df = pd.DataFrame(df_list)
        if df.empty:
            yield {"event": "error", "data": {"ok": False, "error": "Empty board data"}}
            return
        # get top N by weight
        if 'weight' in df.columns:
            df = df.sort_values('weight', ascending=False).head(topn)
        else:
            df = df.head(topn)
        symbols = df['symbol'].astype(str).tolist()
        # try to get realtime quotes
        if not adapter_priority or 'qmt' in adapter_priority:
            try:
                from .adapters.qmt_adapter import subscribe_quotes  # type: ignore
                await subscribe_quotes(symbols)
            except Exception:
                pass
        failures = 0
        update_count = 0
        try:
            while True:
                try:
                    # try qmt first if available
                    if not adapter_priority or 'qmt' in adapter_priority:
                        try:
                            from .adapters.qmt_adapter import fetch_realtime_quotes  # type: ignore
                            tag, qdf = await fetch_realtime_quotes(symbols)
                            q = pd.DataFrame(qdf)
                            if not q.empty:
                                yield {"event": "update", "data": q.to_dict(orient='records')}
                                continue
                        except Exception:
                            pass
                    # fallback to other adapters
                    for adpt in (adapter_priority or ['akshare','ibkr','qstock','efinance','adata']):
                        try:
                            ds = 'securities.equity.cn.quote' if adpt == 'akshare' else f'securities.equity.cn.quote.{adpt}'
                            env = await fetch_data(ds, {})
                            q = pd.DataFrame(env.data)
                            if not q.empty:
                                q = q[q['symbol'].astype(str).isin(symbols)]
                                if not q.empty:
                                    yield {"event": "update", "data": q.to_dict(orient='records')}
                                    continue
                        except Exception:
                            continue
                    failures += 1
                    if failures > 3:
                        yield {"event": "error", "data": {"ok": False, "error": "Too many failures"}}
                        break
                    await asyncio.sleep(interval_sec)
                except Exception as e:
                    yield {"event": "error", "data": {"ok": False, "error": str(e)}}
                    break
                update_count += 1
                if max_updates and update_count >= max_updates:
                    break
        finally:
            try:
                from .adapters.qmt_adapter import unsubscribe_quotes  # type: ignore
                await unsubscribe_quotes(symbols)
            except Exception:
                pass
    return EventSourceResponse(gen())


@app.get("/rpc/stream/index")
async def rpc_stream_index(
    index_codes: List[str] = Query(...),
    topn: int = Query(5),
    adapter_priority: Optional[List[str]] = Query(None),
    weight_by: str = Query("none"),
    interval_sec: int = Query(60),
    max_updates: Optional[int] = Query(None),
    include_percentiles: bool = True,
) -> EventSourceResponse:
    async def gen():
        status = await test_qmt_import()
        if not status.get("ok") and (not adapter_priority or adapter_priority and adapter_priority[0] == 'qmt'):
            yield {"event": "error", "data": {"ok": False, "error": status.get("error"), "is_windows": status.get("is_windows")}}
            return
        # get constituents for each index
        groups: Dict[str, list] = {}
        for idx in index_codes:
            try:
                env = await fetch_data('market.index.constituents.qmt', {"index_code": idx})
                df = pd.DataFrame(env.data)
                groups[idx] = df['symbol'].astype(str).tolist() if not df.empty else []
            except Exception:
                groups[idx] = []
        if not any(groups.values()):
            yield {"event": "error", "data": {"ok": False, "error": "No index constituents available"}}
            return
        # get top N by weight
        symbols = []
        for idx, syms in groups.items():
            if syms:
                symbols.extend(syms[:topn])
        symbols = list(set(symbols))  # deduplicate
        # try to get realtime quotes
        if not adapter_priority or 'qmt' in adapter_priority:
            try:
                from .adapters.qmt_adapter import subscribe_quotes  # type: ignore
                await subscribe_quotes(symbols)
            except Exception:
                pass
        failures = 0
        update_count = 0
        try:
            while True:
                try:
                    # try qmt first if available
                    if not adapter_priority or 'qmt' in adapter_priority:
                        try:
                            from .adapters.qmt_adapter import fetch_realtime_quotes  # type: ignore
                            tag, qdf = await fetch_realtime_quotes(symbols)
                            q = pd.DataFrame(qdf)
                            if not q.empty:
                                yield {"event": "update", "data": q.to_dict(orient='records')}
                                continue
                        except Exception:
                            pass
                    # fallback to other adapters
                    for adpt in (adapter_priority or ['akshare','ibkr','qstock','efinance','adata']):
                        try:
                            ds = 'securities.equity.cn.quote' if adpt == 'akshare' else f'securities.equity.cn.quote.{adpt}'
                            dq = pd.DataFrame(env.data)
                            if not dq.empty:
                                dq = dq[dq['symbol'].astype(str).isin(symbols)]
                                if not dq.empty:
                                    yield {"event": "update", "data": dq.to_dict(orient='records')}
                                    continue
                        except Exception:
                            continue
                    failures += 1
                    if failures > 3:
                        yield {"event": "error", "data": {"ok": False, "error": "Too many failures"}}
                        break
                    await asyncio.sleep(interval_sec)
                except Exception as e:
                    yield {"event": "error", "data": {"ok": False, "error": str(e)}}
                    break
                update_count += 1
                if max_updates and update_count >= max_updates:
                    break
        finally:
            try:
                from .adapters.qmt_adapter import unsubscribe_quotes  # type: ignore
                await unsubscribe_quotes(symbols)
            except Exception:
                pass
    return EventSourceResponse(gen())


@app.get("/rpc/agg/board_playback")
async def rpc_board_playback(
    board_kind: str = Query("industry"),
    boards: List[str] = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    freq: Optional[str] = Query(None),
    window_n: int = Query(10),
    adapter_priority: Optional[List[str]] = Query(None),
    weight_by: str = Query("none"),
) -> Dict[str, Any]:
    """Get board-level aggregated time series data."""
    # get constituents for each board
    async def fetch_cons_one(b: str) -> pd.DataFrame:
        for adpt in (adapter_priority or ['akshare','ibkr','qstock','efinance','adata']):
            try:
                ds = 'securities.board.cn.industry.cons' if board_kind.lower().startswith('i') else 'securities.board.cn.concept.cons'
                ds = ds if adpt == 'akshare' else f"{ds}.{adpt}"
                try:
                    env = await fetch_data(ds, {"board_code": b})
                    df = pd.DataFrame(env.data)
                    if not df.empty and 'symbol' in df.columns:
                        return df[['symbol','weight']] if 'weight' in df.columns else df[['symbol']]
                except Exception:
                    continue
            except Exception:
                continue
        return pd.DataFrame([])
    
    groups: Dict[str, list] = {}
    weights_map: Dict[str, Dict[str, float]] = {}
    for b in boards:
        df = await fetch_cons_one(b)
        groups[b] = df['symbol'].astype(str).tolist() if not df.empty else []
        if not df.empty and 'weight' in df.columns:
            w = pd.to_numeric(df['weight'], errors='coerce')
            weights_map[b] = dict(zip(df['symbol'].astype(str), w))
    
    if not any(groups.values()):
        return {"error": "No board constituents available"}
    
    # get quotes for all symbols
    symbols = list(set([s for syms in groups.values() for s in syms]))
    quotes = {}
    for adpt in (adapter_priority or ['akshare','ibkr','qstock','efinance','adata']):
        ds = 'securities.equity.cn.quote' if adpt == 'akshare' else f'securities.equity.cn.quote.{adpt}'
        try:
            env = await fetch_data(ds, {})
            q = pd.DataFrame(env.data)
            if not q.empty and 'symbol' in q.columns:
                q = q[q['symbol'].astype(str).isin(symbols)]
                if not q.empty:
                    quotes.update(dict(zip(q['symbol'].astype(str), q['close'])))
                    break
        except Exception:
            continue
    
    if not quotes:
        return {"error": "No quote data available"}
    
    # calculate board-level metrics
    board_metrics = {}
    for board, syms in groups.items():
        if not syms:
            continue
        board_quotes = {s: quotes.get(s, 0) for s in syms if s in quotes}
        if not board_quotes:
            continue
        if weight_by == 'weight' and board in weights_map:
            # weighted average
            total_weight = sum(weights_map[board].get(s, 1) for s in board_quotes.keys())
            if total_weight > 0:
                weighted_sum = sum(quotes[s] * weights_map[board].get(s, 1) for s in board_quotes.keys())
                board_metrics[board] = weighted_sum / total_weight
        else:
            # simple average
            board_metrics[board] = sum(board_quotes.values()) / len(board_quotes)
    
    return {
        "board_metrics": board_metrics,
        "total_boards": len(board_metrics),
        "total_symbols": len(symbols),
        "quotes_available": len(quotes)
    }


@app.get("/rpc/agg/index_playback")
async def rpc_index_playback(
    index_codes: List[str] = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    freq: Optional[str] = Query(None),
    window_n: int = Query(10),
    adapter_priority: Optional[List[str]] = Query(None),
    weight_by: str = Query("none"),
) -> Dict[str, Any]:
    """Get index-level aggregated time series data."""
    # get constituents for each index
    async def fetch_cons(idx: str) -> pd.DataFrame:
        for adpt in (adapter_priority or ['akshare','ibkr','qstock','efinance','adata']):
            try:
                ds = 'market.index.constituents' if adpt == 'akshare' else f'market.index.constituents.{adpt}'
                env = await fetch_data(ds, {"index_code": idx})
                df = pd.DataFrame(env.data)
                if not df.empty and 'symbol' in df.columns:
                    return df[['symbol','weight']] if 'weight' in df.columns else df[['symbol']]
            except Exception:
                continue
        return pd.DataFrame([])
    
    groups: Dict[str, list] = {}
    weights_map: Dict[str, Dict[str, float]] = {}
    for idx in index_codes:
        df = await fetch_cons(idx)
        groups[idx] = df['symbol'].astype(str).tolist() if not df.empty else []
        if not df.empty and 'weight' in df.columns:
            w = pd.to_numeric(df['weight'], errors='coerce')
            weights_map[idx] = dict(zip(df['symbol'].astype(str), w))
    
    if not any(groups.values()):
        return {"error": "No index constituents available"}
    
    # get quotes for all symbols
    symbols = list(set([s for syms in groups.values() for s in syms]))
    quotes = {}
    for adpt in (adapter_priority or ['akshare','ibkr','qstock','efinance','adata']):
        ds = 'securities.equity.cn.quote' if adpt == 'akshare' else f'securities.equity.cn.quote.{adpt}'
        try:
            env = await fetch_data(ds, {})
            dq = pd.DataFrame(env.data)
            if not dq.empty and 'symbol' in dq.columns:
                dq = dq[dq['symbol'].astype(str).isin(symbols)]
                if not dq.empty:
                    quotes.update(dict(zip(dq['symbol'].astype(str), dq['close'])))
                    break
        except Exception:
            continue
    
    if not quotes:
        return {"error": "No quote data available"}
    
    # calculate index-level metrics
    index_metrics = {}
    for index, syms in groups.items():
        if not syms:
            continue
        index_quotes = {s: quotes.get(s, 0) for s in syms if s in quotes}
        if not index_quotes:
            continue
        if weight_by == 'weight' and index in weights_map:
            # weighted average
            total_weight = sum(weights_map[index].get(s, 1) for s in index_quotes.keys())
            if total_weight > 0:
                weighted_sum = sum(quotes[s] * weights_map[index].get(s, 1) for s in index_quotes.keys())
                index_metrics[index] = weighted_sum / total_weight
        else:
            # simple average
            index_metrics[index] = sum(index_quotes.values()) / len(index_quotes)
    
    return {
        "index_metrics": index_metrics,
        "total_indices": len(index_metrics),
        "total_symbols": len(symbols),
        "quotes_available": len(quotes)
    }


# ============================================================================
# Earnings Calendar API Endpoints
# ============================================================================

@app.get("/rpc/earnings/calendar")
async def get_earnings_calendar(
    market: str = Query("cn", description="Market code (cn, us, hk)"),
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    symbols: Optional[List[str]] = Query(None, description="List of stock symbols to filter")
) -> EarningsCalendarResponse:
    """Get earnings calendar for specified market and date range."""
    try:
        # Get earnings calendar data
        function_name, df = await call_earnings_calendar(
            'earnings_calendar',
            {
                'market': market,
                'start_date': start_date,
                'end_date': end_date,
                'symbols': symbols
            }
        )
        
        if df.empty:
            return EarningsCalendarResponse(
                success=True,
                data=[],
                total_count=0,
                market=market,
                period=f"{start_date or 'all'} to {end_date or 'all'}",
                source="earnings_calendar_adapter"
            )
        
        # Convert DataFrame to EarningsEvent objects
        events = []
        for _, row in df.iterrows():
            event = EarningsEvent(
                symbol=row.get('symbol', ''),
                company_name=row.get('company_name'),
                report_period=row.get('report_period', ''),
                report_type=row.get('report_type', ''),
                scheduled_date=pd.to_datetime(row.get('report_date')) if row.get('report_date') else None,
                actual_date=pd.to_datetime(row.get('report_date')) if row.get('report_date') else None,
                eps_estimate=row.get('eps_estimate'),
                eps_actual=row.get('eps_actual'),
                revenue_estimate=row.get('revenue_estimate'),
                revenue_actual=row.get('revenue_actual'),
                source=row.get('source', 'earnings_calendar_adapter'),
                market=market
            )
            events.append(event)
        
        return EarningsCalendarResponse(
            success=True,
            data=events,
            total_count=len(events),
            market=market,
            period=f"{start_date or 'all'} to {end_date or 'all'}",
            source="earnings_calendar_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get earnings calendar: {e}")
        return EarningsCalendarResponse(
            success=False,
            data=[],
            total_count=0,
            market=market,
            period=f"{start_date or 'all'} to {end_date or 'all'}",
            source="earnings_calendar_adapter"
        )


@app.get("/rpc/earnings/forecast")
async def get_earnings_forecast(
    symbol: str = Query(..., description="Stock symbol"),
    market: str = Query("cn", description="Market code (cn, us, hk)"),
    period: Optional[str] = Query(None, description="Forecast period")
) -> EarningsForecastResponse:
    """Get earnings forecast for a specific symbol."""
    try:
        # Get earnings forecast data
        function_name, df = await call_earnings_calendar(
            'earnings_forecast',
            {
                'symbol': symbol,
                'market': market,
                'period': period
            }
        )
        
        if df.empty:
            return EarningsForecastResponse(
                success=True,
                symbol=symbol,
                forecasts=[],
                total_count=0,
                market=market,
                source="earnings_calendar_adapter"
            )
        
        # Convert DataFrame to EarningsForecast objects
        forecasts = []
        for _, row in df.iterrows():
            forecast = EarningsForecast(
                symbol=row.get('symbol', symbol),
                forecast_period=row.get('forecast_period', ''),
                forecast_type=row.get('forecast_type', ''),
                net_profit_change=row.get('net_profit_change'),
                change_reason=row.get('change_reason'),
                announcement_date=pd.to_datetime(row.get('announcement_date')) if row.get('announcement_date') else datetime.now(),
                source=row.get('source', 'earnings_calendar_adapter'),
                market=market
            )
            forecasts.append(forecast)
        
        return EarningsForecastResponse(
            success=True,
            symbol=symbol,
            forecasts=forecasts,
            total_count=len(forecasts),
            market=market,
            source="earnings_calendar_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get earnings forecast for {symbol}: {e}")
        return EarningsForecastResponse(
            success=False,
            symbol=symbol,
            forecasts=[],
            total_count=0,
            market=market,
            source="earnings_calendar_adapter"
        )


@app.get("/rpc/earnings/dates")
async def get_earnings_dates(
    symbol: str = Query(..., description="Stock symbol"),
    market: str = Query("cn", description="Market code (cn, us, hk)")
) -> EarningsCalendarResponse:
    """Get earnings dates for a specific symbol."""
    try:
        # Get earnings dates data
        function_name, df = await call_earnings_calendar(
            'earnings_dates',
            {
                'symbol': symbol,
                'market': market
            }
        )
        
        if df.empty:
            return EarningsCalendarResponse(
                success=True,
                data=[],
                total_count=0,
                market=market,
                period="all",
                source="earnings_calendar_adapter"
            )
        
        # Convert DataFrame to EarningsEvent objects
        events = []
        for _, row in df.iterrows():
            event = EarningsEvent(
                symbol=row.get('symbol', symbol),
                company_name=row.get('company_name'),
                report_period=row.get('report_period', ''),
                report_type=row.get('report_type', ''),
                scheduled_date=pd.to_datetime(row.get('report_date')) if row.get('report_date') else None,
                actual_date=pd.to_datetime(row.get('report_date')) if row.get('report_date') else None,
                source=row.get('source', 'earnings_calendar_adapter'),
                market=market
            )
            events.append(event)
        
        return EarningsCalendarResponse(
            success=True,
            data=events,
            total_count=len(events),
            market=market,
            period="all",
            source="earnings_calendar_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get earnings dates for {symbol}: {e}")
        return EarningsCalendarResponse(
            success=False,
            data=[],
            total_count=0,
            market=market,
            period="all",
            source="earnings_calendar_adapter"
        )


# ============================================================================
# Financial Data API Endpoints
# ============================================================================

@app.get("/rpc/financial/indicators")
async def get_financial_indicators(
    symbol: str = Query(..., description="Stock symbol"),
    market: str = Query("cn", description="Market code (cn, us, hk)"),
    period: str = Query("annual", description="Period type (annual, quarterly)"),
    indicators: Optional[List[str]] = Query(None, description="List of specific indicators to get")
) -> FinancialIndicatorsResponse:
    """Get financial indicators for a specific symbol."""
    try:
        # Get financial indicators data
        function_name, df = await call_financial_data(
            'financial_indicators',
            {
                'symbol': symbol,
                'market': market,
                'period': period,
                'indicators': indicators
            }
        )
        
        if df.empty:
            return FinancialIndicatorsResponse(
                success=True,
                symbol=symbol,
                indicators=[],
                total_count=0,
                period=period,
                market=market,
                source="financial_data_adapter"
            )
        
        # Convert DataFrame to FinancialIndicator objects
        indicator_list = []
        for _, row in df.iterrows():
            # Get all columns except symbol and report_date
            indicator_cols = [col for col in df.columns if col not in ['symbol', 'report_date']]
            
            for col in indicator_cols:
                if pd.notna(row[col]) and row[col] is not None:
                    indicator = FinancialIndicator(
                        symbol=row.get('symbol', symbol),
                        indicator_name=col,
                        indicator_value=float(row[col]) if pd.notna(row[col]) else 0.0,
                        report_date=pd.to_datetime(row.get('report_date')) if row.get('report_date') else datetime.now(),
                        period=period,
                        source="financial_data_adapter",
                        market=market
                    )
                    indicator_list.append(indicator)
        
        return FinancialIndicatorsResponse(
            success=True,
            symbol=symbol,
            indicators=indicator_list,
            total_count=len(indicator_list),
            period=period,
            market=market,
            source="financial_data_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get financial indicators for {symbol}: {e}")
        return FinancialIndicatorsResponse(
            success=False,
            symbol=symbol,
            indicators=[],
            total_count=0,
            period=period,
            market=market,
            source="financial_data_adapter"
        )


@app.get("/rpc/financial/statements")
async def get_financial_statements(
    symbol: str = Query(..., description="Stock symbol"),
    statement_type: str = Query(..., description="Type of statement (balance_sheet, income_statement, cash_flow)"),
    market: str = Query("cn", description="Market code (cn, us, hk)"),
    period: str = Query("annual", description="Period type (annual, quarterly)")
) -> FinancialStatementResponse:
    """Get financial statements for a specific symbol."""
    try:
        # Get financial statements data
        function_name, df = await call_financial_data(
            'financial_statements',
            {
                'symbol': symbol,
                'statement_type': statement_type,
                'market': market,
                'period': period
            }
        )
        
        if df.empty:
            return FinancialStatementResponse(
                success=False,
                symbol=symbol,
                statement_type=statement_type,
                statement=None,
                period=period,
                market=market,
                source="financial_data_adapter"
            )
        
        # Convert DataFrame to appropriate statement object
        # This is a simplified conversion - in practice, you'd want more sophisticated mapping
        statement_data = df.to_dict(orient='records')[0] if not df.empty else {}
        
        # Create a generic FinancialStatement for now
        from .schemas.financial import FinancialStatement
        statement = FinancialStatement(
            symbol=symbol,
            statement_type=statement_type,
            period=period,
            report_date=pd.to_datetime(statement_data.get('report_date')) if statement_data.get('report_date') else datetime.now(),
            data=statement_data,
            source="financial_data_adapter",
            market=market
        )
        
        return FinancialStatementResponse(
            success=True,
            symbol=symbol,
            statement_type=statement_type,
            statement=statement,
            period=period,
            market=market,
            source="financial_data_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get financial statements for {symbol}: {e}")
        return FinancialStatementResponse(
            success=False,
            symbol=symbol,
            statement_type=statement_type,
            statement=None,
            period=period,
            market=market,
            source="financial_data_adapter"
        )


# ============================================================================
# Fund Portfolio API Endpoints
# ============================================================================

@app.get("/rpc/fund/portfolio")
async def get_fund_portfolio(
    fund_code: str = Query(..., description="Fund code"),
    market: str = Query("cn", description="Market code (cn, hk, us)"),
    report_date: Optional[str] = Query(None, description="Report date in YYYY-MM-DD format")
) -> FundPortfolioResponse:
    """Get fund portfolio holdings."""
    try:
        # Get fund portfolio data
        function_name, df = await call_fund_portfolio(
            'fund_portfolio',
            {
                'fund_code': fund_code,
                'market': market,
                'report_date': report_date
            }
        )
        
        if df.empty:
            return FundPortfolioResponse(
                success=False,
                fund_code=fund_code,
                fund_name="",
                portfolio=None,
                total_holdings=0,
                market=market,
                source="fund_portfolio_adapter"
            )
        
        # Convert DataFrame to FundPortfolio object
        from .schemas.fund import FundPortfolio, StockHolding
        
        # Get fund name from first row
        fund_name = df.iloc[0].get('fund_name', '') if not df.empty else ''
        
        # Convert holdings
        stock_holdings = []
        for _, row in df.iterrows():
            holding = StockHolding(
                symbol=row.get('symbol', ''),
                stock_name=row.get('stock_name', ''),
                shares=row.get('shares', 0),
                market_value=row.get('market_value', 0.0),
                percentage=row.get('percentage', 0.0),
                change_shares=row.get('shares_change'),
                change_percentage=row.get('percentage_change'),
                report_date=pd.to_datetime(row.get('report_date')) if row.get('report_date') else datetime.now()
            )
            stock_holdings.append(holding)
        
        portfolio = FundPortfolio(
            fund_code=fund_code,
            fund_name=fund_name,
            report_date=datetime.now(),
            total_assets=None,
            stock_holdings=stock_holdings,
            source="fund_portfolio_adapter",
            market=market
        )
        
        return FundPortfolioResponse(
            success=True,
            fund_code=fund_code,
            fund_name=fund_name,
            portfolio=portfolio,
            total_holdings=len(stock_holdings),
            market=market,
            source="fund_portfolio_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get fund portfolio for {fund_code}: {e}")
        return FundPortfolioResponse(
            success=False,
            fund_code=fund_code,
            fund_name="",
            portfolio=None,
            total_holdings=0,
            market=market,
            source="fund_portfolio_adapter"
        )


@app.get("/rpc/fund/holdings_change")
async def get_fund_holdings_change(
    fund_code: str = Query(..., description="Fund code"),
    market: str = Query("cn", description="Market code (cn, hk, us)"),
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")
) -> FundHoldingsChangeResponse:
    """Get fund holdings changes over time."""
    try:
        # Get fund holdings changes data
        function_name, df = await call_fund_portfolio(
            'fund_holdings_change',
            {
                'fund_code': fund_code,
                'market': market,
                'start_date': start_date,
                'end_date': end_date
            }
        )
        
        if df.empty:
            return FundHoldingsChangeResponse(
                success=False,
                fund_code=fund_code,
                fund_name="",
                changes=[],
                total_changes=0,
                market=market,
                source="fund_portfolio_adapter"
            )
        
        # Convert DataFrame to FundHoldingsChange objects
        from .schemas.fund import FundHoldingsChange
        
        changes = []
        for _, row in df.iterrows():
            change = FundHoldingsChange(
                fund_code=fund_code,
                fund_name=row.get('fund_name', ''),
                symbol=row.get('symbol', ''),
                stock_name=row.get('stock_name', ''),
                report_date=pd.to_datetime(row.get('report_date')) if row.get('report_date') else datetime.now(),
                shares=row.get('shares', 0),
                market_value=row.get('market_value', 0.0),
                percentage=row.get('percentage', 0.0),
                shares_change=row.get('shares_change', 0),
                value_change=row.get('value_change', 0.0),
                percentage_change=row.get('percentage_change', 0.0),
                source="fund_portfolio_adapter",
                market=market
            )
            changes.append(change)
        
        return FundHoldingsChangeResponse(
            success=True,
            fund_code=fund_code,
            fund_name=changes[0].fund_name if changes else "",
            changes=changes,
            total_changes=len(changes),
            market=market,
            source="fund_portfolio_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get fund holdings change for {fund_code}: {e}")
        return FundHoldingsChangeResponse(
            success=False,
            fund_code=fund_code,
            fund_name="",
            changes=[],
            total_changes=0,
            market=market,
            source="fund_portfolio_adapter"
        )


@app.get("/rpc/fund/top_holdings")
async def get_fund_top_holdings(
    fund_code: str = Query(..., description="Fund code"),
    market: str = Query("cn", description="Market code (cn, hk, us)"),
    top_n: int = Query(10, description="Number of top holdings to return")
) -> FundTopHoldingsResponse:
    """Get fund top holdings."""
    try:
        # Get fund top holdings data
        function_name, df = await call_fund_portfolio(
            'fund_top_holdings',
            {
                'fund_code': fund_code,
                'market': market,
                'top_n': top_n
            }
        )
        
        if df.empty:
            return FundTopHoldingsResponse(
                success=False,
                fund_code=fund_code,
                fund_name="",
                top_holdings=None,
                market=market,
                source="fund_portfolio_adapter"
            )
        
        # Convert DataFrame to FundTopHoldings object
        from .schemas.fund import FundTopHoldings, StockHolding
        
        # Get fund name from first row
        fund_name = df.iloc[0].get('fund_name', '') if not df.empty else ''
        
        # Convert top holdings
        top_holdings_list = []
        total_percentage = 0.0
        
        for _, row in df.iterrows():
            holding = StockHolding(
                symbol=row.get('symbol', ''),
                stock_name=row.get('stock_name', ''),
                shares=row.get('shares', 0),
                market_value=row.get('market_value', 0.0),
                percentage=row.get('percentage', 0.0),
                report_date=pd.to_datetime(row.get('report_date')) if row.get('report_date') else datetime.now()
            )
            top_holdings_list.append(holding)
            total_percentage += row.get('percentage', 0.0)
        
        top_holdings = FundTopHoldings(
            fund_code=fund_code,
            fund_name=fund_name,
            report_date=datetime.now(),
            top_holdings=top_holdings_list,
            total_percentage=total_percentage,
            source="fund_portfolio_adapter",
            market=market
        )
        
        return FundTopHoldingsResponse(
            success=True,
            fund_code=fund_code,
            fund_name=fund_name,
            top_holdings=top_holdings,
            market=market,
            source="fund_portfolio_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get fund top holdings for {fund_code}: {e}")
        return FundTopHoldingsResponse(
            success=False,
            fund_code=fund_code,
            fund_name="",
            top_holdings=None,
            market=market,
            source="fund_portfolio_adapter"
        )


# ============================================================================
# Snowball API Endpoints
# ============================================================================

@app.get("/rpc/snowball/quote")
async def get_snowball_quote(
    symbol: str = Query(..., description="Stock symbol"),
    market: str = Query("cn", description="Market code (cn, hk, us)")
) -> SnowballQuoteResponse:
    """Get stock quote from Snowball."""
    try:
        # Get stock quote data
        function_name, df = await call_snowball(
            'stock_quote',
            {
                'symbol': symbol,
                'market': market
            }
        )
        
        if df.empty:
            return SnowballQuoteResponse(
                success=False,
                symbol=symbol,
                quote=None,
                source="snowball_adapter"
            )
        
        # Convert DataFrame to SnowballQuote object
        from .schemas.snowball import SnowballQuote
        
        row = df.iloc[0]
        quote = SnowballQuote(
            symbol=symbol,
            market=market,
            name=row.get('name', ''),
            current=row.get('current', 0.0),
            change=row.get('change', 0.0),
            change_percent=row.get('change_percent', 0.0),
            open=row.get('open', 0.0),
            high=row.get('high', 0.0),
            low=row.get('low', 0.0),
            volume=row.get('volume', 0),
            market_cap=row.get('market_cap', 0.0),
            pe_ratio=row.get('pe_ratio', 0.0),
            pb_ratio=row.get('pb_ratio', 0.0),
            dividend_yield=row.get('dividend_yield', 0.0)
        )
        
        return SnowballQuoteResponse(
            success=True,
            symbol=symbol,
            quote=quote,
            source="snowball_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get Snowball quote for {symbol}: {e}")
        return SnowballQuoteResponse(
            success=False,
            symbol=symbol,
            quote=None,
            source="snowball_adapter"
        )


@app.get("/rpc/snowball/financial_data")
async def get_snowball_financial_data(
    symbol: str = Query(..., description="Stock symbol"),
    market: str = Query("cn", description="Market code (cn, hk, us)"),
    period: str = Query("annual", description="Period type (annual, quarterly)")
) -> SnowballFinancialDataResponse:
    """Get financial data from Snowball."""
    try:
        # Get financial data
        function_name, df = await call_snowball(
            'financial_data',
            {
                'symbol': symbol,
                'market': market,
                'period': period
            }
        )
        
        if df.empty:
            return SnowballFinancialDataResponse(
                success=False,
                symbol=symbol,
                financial_data=[],
                total_count=0,
                source="snowball_adapter"
            )
        
        # Convert DataFrame to SnowballFinancialData objects
        from .schemas.snowball import SnowballFinancialData
        
        financial_data = []
        for _, row in df.iterrows():
            data = SnowballFinancialData(
                symbol=symbol,
                market=market,
                period=row.get('period', ''),
                report_date=row.get('report_date', ''),
                revenue=row.get('revenue', 0.0),
                net_profit=row.get('net_profit', 0.0),
                eps=row.get('eps', 0.0),
                roe=row.get('roe', 0.0),
                roa=row.get('roa', 0.0),
                gross_margin=row.get('gross_margin', 0.0),
                net_margin=row.get('net_margin', 0.0),
                debt_ratio=row.get('debt_ratio', 0.0),
                current_ratio=row.get('current_ratio', 0.0)
            )
            financial_data.append(data)
        
        return SnowballFinancialDataResponse(
            success=True,
            symbol=symbol,
            financial_data=financial_data,
            total_count=len(financial_data),
            source="snowball_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get Snowball financial data for {symbol}: {e}")
        return SnowballFinancialDataResponse(
            success=False,
            symbol=symbol,
            financial_data=[],
            total_count=0,
            source="snowball_adapter"
        )


@app.get("/rpc/snowball/research_reports")
async def get_snowball_research_reports(
    symbol: str = Query(..., description="Stock symbol"),
    market: str = Query("cn", description="Market code (cn, hk, us)"),
    limit: int = Query(20, description="Number of reports to return")
) -> SnowballResearchReportResponse:
    """Get research reports from Snowball."""
    try:
        # Get research reports
        function_name, df = await call_snowball(
            'research_reports',
            {
                'symbol': symbol,
                'market': market,
                'limit': limit
            }
        )
        
        if df.empty:
            return SnowballResearchReportResponse(
                success=False,
                symbol=symbol,
                reports=[],
                total_count=0,
                source="snowball_adapter"
            )
        
        # Convert DataFrame to SnowballResearchReport objects
        from .schemas.snowball import SnowballResearchReport
        
        reports = []
        for _, row in df.iterrows():
            report = SnowballResearchReport(
                symbol=symbol,
                market=market,
                title=row.get('title', ''),
                author=row.get('author', ''),
                institution=row.get('institution', ''),
                publish_date=row.get('publish_date', ''),
                rating=row.get('rating', ''),
                target_price=row.get('target_price', 0.0),
                summary=row.get('summary', ''),
                url=row.get('url', '')
            )
            reports.append(report)
        
        return SnowballResearchReportResponse(
            success=True,
            symbol=symbol,
            reports=reports,
            total_count=len(reports),
            source="snowball_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get Snowball research reports for {symbol}: {e}")
        return SnowballResearchReportResponse(
            success=False,
            symbol=symbol,
            reports=[],
            total_count=0,
            source="snowball_adapter"
        )


@app.get("/rpc/snowball/sentiment")
async def get_snowball_sentiment(
    symbol: str = Query(..., description="Stock symbol"),
    market: str = Query("cn", description="Market code (cn, hk, us)"),
    days: int = Query(7, description="Number of days to analyze")
) -> SnowballSentimentResponse:
    """Get sentiment data from Snowball."""
    try:
        # Get sentiment data
        function_name, df = await call_snowball(
            'sentiment',
            {
                'symbol': symbol,
                'market': market,
                'days': days
            }
        )
        
        if df.empty:
            return SnowballSentimentResponse(
                success=False,
                symbol=symbol,
                sentiment_data=[],
                total_count=0,
                source="snowball_adapter"
            )
        
        # Convert DataFrame to SnowballSentiment objects
        from .schemas.snowball import SnowballSentiment
        
        sentiment_data = []
        for _, row in df.iterrows():
            sentiment = SnowballSentiment(
                symbol=symbol,
                market=market,
                date=row.get('date', ''),
                positive_count=row.get('positive_count', 0),
                negative_count=row.get('negative_count', 0),
                neutral_count=row.get('neutral_count', 0),
                sentiment_score=row.get('sentiment_score', 0.0),
                discussion_count=row.get('discussion_count', 0)
            )
            sentiment_data.append(sentiment)
        
        return SnowballSentimentResponse(
            success=True,
            symbol=symbol,
            sentiment_data=sentiment_data,
            total_count=len(sentiment_data),
            source="snowball_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get Snowball sentiment for {symbol}: {e}")
        return SnowballSentimentResponse(
            success=False,
            symbol=symbol,
            sentiment_data=[],
            total_count=0,
            source="snowball_adapter"
        )


@app.get("/rpc/snowball/discussions")
async def get_snowball_discussions(
    symbol: str = Query(..., description="Stock symbol"),
    market: str = Query("cn", description="Market code (cn, hk, us)"),
    limit: int = Query(50, description="Number of discussions to return")
) -> SnowballDiscussionResponse:
    """Get discussions from Snowball."""
    try:
        # Get discussions
        function_name, df = await call_snowball(
            'discussions',
            {
                'symbol': symbol,
                'market': market,
                'limit': limit
            }
        )
        
        if df.empty:
            return SnowballDiscussionResponse(
                success=False,
                symbol=symbol,
                discussions=[],
                total_count=0,
                source="snowball_adapter"
            )
        
        # Convert DataFrame to SnowballDiscussion objects
        from .schemas.snowball import SnowballDiscussion
        
        discussions = []
        for _, row in df.iterrows():
            discussion = SnowballDiscussion(
                symbol=symbol,
                market=market,
                title=row.get('title', ''),
                content=row.get('content', ''),
                author=row.get('author', ''),
                publish_time=row.get('publish_time', ''),
                like_count=row.get('like_count', 0),
                comment_count=row.get('comment_count', 0),
                share_count=row.get('share_count', 0),
                sentiment=row.get('sentiment', ''),
                url=row.get('url', '')
            )
            discussions.append(discussion)
        
        return SnowballDiscussionResponse(
            success=True,
            symbol=symbol,
            discussions=discussions,
            total_count=len(discussions),
            source="snowball_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get Snowball discussions for {symbol}: {e}")
        return SnowballDiscussionResponse(
            success=False,
            symbol=symbol,
            discussions=[],
            total_count=0,
            source="snowball_adapter"
        )


@app.get("/rpc/snowball/market_overview")
async def get_snowball_market_overview(
    market: str = Query("cn", description="Market code (cn, hk, us)")
) -> SnowballMarketOverviewResponse:
    """Get market overview from Snowball."""
    try:
        # Get market overview
        function_name, df = await call_snowball(
            'market_overview',
            {
                'market': market
            }
        )
        
        if df.empty:
            return SnowballMarketOverviewResponse(
                success=False,
                market=market,
                overview=None,
                source="snowball_adapter"
            )
        
        # Convert DataFrame to SnowballMarketOverview object
        from .schemas.snowball import SnowballMarketOverview
        
        row = df.iloc[0]
        overview = SnowballMarketOverview(
            market=market,
            index_name=row.get('index_name', ''),
            current_value=row.get('current_value', 0.0),
            change=row.get('change', 0.0),
            change_percent=row.get('change_percent', 0.0),
            volume=row.get('volume', 0),
            turnover=row.get('turnover', 0.0),
            advance_count=row.get('advance_count', 0),
            decline_count=row.get('decline_count', 0),
            flat_count=row.get('flat_count', 0)
        )
        
        return SnowballMarketOverviewResponse(
            success=True,
            market=market,
            overview=overview,
            source="snowball_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get Snowball market overview for {market}: {e}")
        return SnowballMarketOverviewResponse(
            success=False,
            market=market,
            overview=None,
            source="snowball_adapter"
        )


# ============================================================================
# EasyTrader API Endpoints
# ============================================================================

@app.post("/rpc/easytrader/login")
async def easytrader_login(
    request: EasyTraderLoginRequest
) -> EasyTraderLoginResponse:
    """Login to EasyTrader account."""
    try:
        # Login to EasyTrader
        function_name, df = await call_easytrader(
            'login',
            {
                'username': request.username,
                'password': request.password,
                'exe_path': request.exe_path,
                'comm_password': request.comm_password
            },
            broker=request.broker
        )
        
        success = df.iloc[0]['success'] if not df.empty else False
        
        return EasyTraderLoginResponse(
            success=success,
            broker=request.broker,
            message="Login successful" if success else "Login failed"
        )
        
    except Exception as e:
        logger.error(f"Failed to login to EasyTrader {request.broker}: {e}")
        return EasyTraderLoginResponse(
            success=False,
            broker=request.broker,
            message=f"Login failed: {str(e)}"
        )


@app.get("/rpc/easytrader/account_info")
async def get_easytrader_account_info(
    broker: str = Query(..., description="Broker name")
) -> EasyTraderAccountInfoResponse:
    """Get account information from EasyTrader."""
    try:
        # Get account info
        function_name, df = await call_easytrader(
            'account_info',
            {},
            broker=broker
        )
        
        if df.empty:
            return EasyTraderAccountInfoResponse(
                success=False,
                broker=broker,
                account_info=None,
                source="easytrader_adapter"
            )
        
        # Convert DataFrame to EasyTraderAccountInfo object
        from .schemas.easytrader import EasyTraderAccountInfo
        
        row = df.iloc[0]
        account_info = EasyTraderAccountInfo(
            broker=broker,
            total_assets=row.get('total_assets', 0.0),
            available_cash=row.get('available_cash', 0.0),
            market_value=row.get('market_value', 0.0),
            frozen_cash=row.get('frozen_cash', 0.0),
            total_profit_loss=row.get('total_profit_loss', 0.0),
            today_profit_loss=row.get('today_profit_loss', 0.0)
        )
        
        return EasyTraderAccountInfoResponse(
            success=True,
            broker=broker,
            account_info=account_info,
            source="easytrader_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get EasyTrader account info for {broker}: {e}")
        return EasyTraderAccountInfoResponse(
            success=False,
            broker=broker,
            account_info=None,
            source="easytrader_adapter"
        )


@app.get("/rpc/easytrader/portfolio")
async def get_easytrader_portfolio(
    broker: str = Query(..., description="Broker name")
) -> EasyTraderPortfolioResponse:
    """Get portfolio from EasyTrader."""
    try:
        # Get portfolio
        function_name, df = await call_easytrader(
            'portfolio',
            {},
            broker=broker
        )
        
        if df.empty:
            return EasyTraderPortfolioResponse(
                success=False,
                broker=broker,
                positions=[],
                total_count=0,
                source="easytrader_adapter"
            )
        
        # Convert DataFrame to EasyTraderPosition objects
        from .schemas.easytrader import EasyTraderPosition
        
        positions = []
        for _, row in df.iterrows():
            position = EasyTraderPosition(
                broker=broker,
                symbol=row.get('symbol', ''),
                name=row.get('name', ''),
                shares=row.get('shares', 0),
                available_shares=row.get('available_shares', 0),
                cost_price=row.get('cost_price', 0.0),
                current_price=row.get('current_price', 0.0),
                market_value=row.get('market_value', 0.0),
                profit_loss=row.get('profit_loss', 0.0),
                profit_loss_ratio=row.get('profit_loss_ratio', 0.0)
            )
            positions.append(position)
        
        return EasyTraderPortfolioResponse(
            success=True,
            broker=broker,
            positions=positions,
            total_count=len(positions),
            source="easytrader_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get EasyTrader portfolio for {broker}: {e}")
        return EasyTraderPortfolioResponse(
            success=False,
            broker=broker,
            positions=[],
            total_count=0,
            source="easytrader_adapter"
        )


@app.get("/rpc/easytrader/trading_history")
async def get_easytrader_trading_history(
    broker: str = Query(..., description="Broker name"),
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")
) -> EasyTraderTradingHistoryResponse:
    """Get trading history from EasyTrader."""
    try:
        # Get trading history
        function_name, df = await call_easytrader(
            'trading_history',
            {
                'start_date': start_date,
                'end_date': end_date
            },
            broker=broker
        )
        
        if df.empty:
            return EasyTraderTradingHistoryResponse(
                success=False,
                broker=broker,
                trades=[],
                total_count=0,
                source="easytrader_adapter"
            )
        
        # Convert DataFrame to EasyTraderTrade objects
        from .schemas.easytrader import EasyTraderTrade
        
        trades = []
        for _, row in df.iterrows():
            trade = EasyTraderTrade(
                broker=broker,
                trade_date=row.get('trade_date', ''),
                symbol=row.get('symbol', ''),
                name=row.get('name', ''),
                trade_type=row.get('trade_type', ''),
                shares=row.get('shares', 0),
                price=row.get('price', 0.0),
                amount=row.get('amount', 0.0),
                commission=row.get('commission', 0.0),
                stamp_duty=row.get('stamp_duty', 0.0)
            )
            trades.append(trade)
        
        return EasyTraderTradingHistoryResponse(
            success=True,
            broker=broker,
            trades=trades,
            total_count=len(trades),
            source="easytrader_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get EasyTrader trading history for {broker}: {e}")
        return EasyTraderTradingHistoryResponse(
            success=False,
            broker=broker,
            trades=[],
            total_count=0,
            source="easytrader_adapter"
        )


@app.get("/rpc/easytrader/market_data")
async def get_easytrader_market_data(
    broker: str = Query(..., description="Broker name"),
    symbols: List[str] = Query(..., description="List of stock symbols")
) -> EasyTraderMarketDataResponse:
    """Get market data from EasyTrader."""
    try:
        # Get market data
        function_name, df = await call_easytrader(
            'market_data',
            {
                'symbols': symbols
            },
            broker=broker
        )
        
        if df.empty:
            return EasyTraderMarketDataResponse(
                success=False,
                broker=broker,
                market_data=[],
                total_count=0,
                source="easytrader_adapter"
            )
        
        # Convert DataFrame to EasyTraderMarketData objects
        from .schemas.easytrader import EasyTraderMarketData
        
        market_data = []
        for _, row in df.iterrows():
            data = EasyTraderMarketData(
                broker=broker,
                symbol=row.get('symbol', ''),
                name=row.get('name', ''),
                current_price=row.get('current_price', 0.0),
                change=row.get('change', 0.0),
                change_percent=row.get('change_percent', 0.0),
                open=row.get('open', 0.0),
                high=row.get('high', 0.0),
                low=row.get('low', 0.0),
                volume=row.get('volume', 0),
                turnover=row.get('turnover', 0.0)
            )
            market_data.append(data)
        
        return EasyTraderMarketDataResponse(
            success=True,
            broker=broker,
            market_data=market_data,
            total_count=len(market_data),
            source="easytrader_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get EasyTrader market data for {broker}: {e}")
        return EasyTraderMarketDataResponse(
            success=False,
            broker=broker,
            market_data=[],
            total_count=0,
            source="easytrader_adapter"
        )


@app.get("/rpc/easytrader/fund_info")
async def get_easytrader_fund_info(
    broker: str = Query(..., description="Broker name")
) -> EasyTraderFundInfoResponse:
    """Get fund information from EasyTrader."""
    try:
        # Get fund info
        function_name, df = await call_easytrader(
            'fund_info',
            {},
            broker=broker
        )
        
        if df.empty:
            return EasyTraderFundInfoResponse(
                success=False,
                broker=broker,
                fund_info=None,
                source="easytrader_adapter"
            )
        
        # Convert DataFrame to EasyTraderFundInfo object
        from .schemas.easytrader import EasyTraderFundInfo
        
        row = df.iloc[0]
        fund_info = EasyTraderFundInfo(
            broker=broker,
            fund_code=row.get('fund_code', ''),
            fund_name=row.get('fund_name', ''),
            branch=row.get('branch', ''),
            status=row.get('status', '')
        )
        
        return EasyTraderFundInfoResponse(
            success=True,
            broker=broker,
            fund_info=fund_info,
            source="easytrader_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get EasyTrader fund info for {broker}: {e}")
        return EasyTraderFundInfoResponse(
            success=False,
            broker=broker,
            fund_info=None,
            source="easytrader_adapter"
        )


@app.get("/rpc/easytrader/risk_metrics")
async def get_easytrader_risk_metrics(
    broker: str = Query(..., description="Broker name")
) -> EasyTraderRiskMetricsResponse:
    """Get risk metrics from EasyTrader."""
    try:
        # Get risk metrics
        function_name, df = await call_easytrader(
            'risk_metrics',
            {},
            broker=broker
        )
        
        if df.empty:
            return EasyTraderRiskMetricsResponse(
                success=False,
                broker=broker,
                risk_metrics=None,
                source="easytrader_adapter"
            )
        
        # Convert DataFrame to EasyTraderRiskMetrics object
        from .schemas.easytrader import EasyTraderRiskMetrics
        
        row = df.iloc[0]
        risk_metrics = EasyTraderRiskMetrics(
            broker=broker,
            var=row.get('var', 0.0),
            max_drawdown=row.get('max_drawdown', 0.0),
            sharpe_ratio=row.get('sharpe_ratio', 0.0),
            volatility=row.get('volatility', 0.0),
            beta=row.get('beta', 0.0)
        )
        
        return EasyTraderRiskMetricsResponse(
            success=True,
            broker=broker,
            risk_metrics=risk_metrics,
            source="easytrader_adapter"
        )
        
    except Exception as e:
        logger.error(f"Failed to get EasyTrader risk metrics for {broker}: {e}")
        return EasyTraderRiskMetricsResponse(
            success=False,
            broker=broker,
            risk_metrics=None,
            source="easytrader_adapter"
        )