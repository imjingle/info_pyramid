from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, List

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
from .adapters.qmt_adapter import test_qmt_import  # type: ignore
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
async def admin_cache_blob_get(dataset_id: str = Query(...), params: Dict[str, Any] = Query(...)) -> Dict[str, Any]:
    pool = await _get_pool()
    if pool is None:
        return {"enabled": False}
    res = await _blob_fetch(pool, dataset_id, params)
    if res is None:
        return {"enabled": True, "found": False}
    raw_obj, meta = res
    return {"enabled": True, "found": True, "raw": raw_obj, "meta": meta}

@app.post("/admin/cache/blob/purge")
async def admin_cache_blob_purge(
    dataset_id: Optional[str] = Query(None),
    params: Optional[Dict[str, Any]] = Query(None),
    dataset_prefix: Optional[str] = Query(None),
    updated_after: Optional[str] = Query(None),
    updated_before: Optional[str] = Query(None),
) -> Dict[str, Any]:
    pool = await _get_pool()
    if pool is None:
        return {"enabled": False, "deleted": 0}
    deleted = await _blob_purge(pool, dataset_id, params, dataset_prefix=dataset_prefix, updated_after=updated_after, updated_before=updated_before)
    return {"enabled": True, "deleted": int(deleted)}

@app.get("/rpc/replay")
async def rpc_replay(dataset_id: str = Query(...), params: Dict[str, Any] = Query(...), format: str = Query("raw")) -> Dict[str, Any]:
    pool = await _get_pool()
    if pool is None:
        return {"ok": False, "error": "cache disabled"}
    
    try:
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
    params: Dict[str, Any] = Query(...),
    interval_sec: int = Query(60),
    max_updates: Optional[int] = Query(None),
    adapter: Optional[str] = Query(None),
) -> EventSourceResponse:
    """Stream real-time data updates."""
    async def gen():
        update_count = 0
        try:
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
        df = _pd.DataFrame(df_list)
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
                            q = _pd.DataFrame(qdf)
                            if not q.empty:
                                return q
                        except Exception:
                            pass
                    # fallback to other adapters
                    for adpt in (adapter_priority or ['akshare','ibkr','qstock','efinance','adata']):
                        try:
                            ds = 'securities.equity.cn.quote' if adpt == 'akshare' else f'securities.equity.cn.quote.{adpt}'
                            env = await fetch_data(ds, {})
                            q = _pd.DataFrame(env.data)
                            if not q.empty:
                                q = q[q['symbol'].astype(str).isin(symbols)]
                                if not q.empty:
                                    return q
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
                df = _pd.DataFrame(env.data)
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
                            q = _pd.DataFrame(qdf)
                            if not q.empty:
                                return q
                        except Exception:
                            pass
                    # fallback to other adapters
                    for adpt in (adapter_priority or ['akshare','ibkr','qstock','efinance','adata']):
                        try:
                            ds = 'securities.equity.cn.quote' if adpt == 'akshare' else f'securities.equity.cn.quote.{adpt}'
                            dq = _pd.DataFrame(env.data)
                            if not dq.empty:
                                dq = dq[dq['symbol'].astype(str).isin(symbols)]
                                if not dq.empty:
                                    return dq
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
    def fetch_cons_one(b: str) -> _pd.DataFrame:
        for adpt in (adapter_priority or ['akshare','ibkr','qstock','efinance','adata']):
            try:
                ds = 'securities.board.cn.industry.cons' if board_kind.lower().startswith('i') else 'securities.board.cn.concept.cons'
                ds = ds if adpt == 'akshare' else f"{ds}.{adpt}"
                try:
                    env = await fetch_data(ds, {"board_code": b})
                    df = _pd.DataFrame(env.data)
                    if not df.empty and 'symbol' in df.columns:
                        return df[['symbol','weight']] if 'weight' in df.columns else df[['symbol']]
                except Exception:
                    continue
            except Exception:
                continue
        return _pd.DataFrame([])
    
    groups: Dict[str, list] = {}
    weights_map: Dict[str, Dict[str, float]] = {}
    for b in boards:
        df = await fetch_cons_one(b)
        groups[b] = df['symbol'].astype(str).tolist() if not df.empty else []
        if not df.empty and 'weight' in df.columns:
            w = _pd.to_numeric(df['weight'], errors='coerce')
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
            q = _pd.DataFrame(env.data)
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
    def fetch_cons(idx: str) -> _pd.DataFrame:
        for adpt in (adapter_priority or ['akshare','ibkr','qstock','efinance','adata']):
            try:
                ds = 'market.index.constituents' if adpt == 'akshare' else f'market.index.constituents.{adpt}'
                env = await fetch_data(ds, {"index_code": idx})
                df = _pd.DataFrame(env.data)
                if not df.empty and 'symbol' in df.columns:
                    return df[['symbol','weight']] if 'weight' in df.columns else df[['symbol']]
            except Exception:
                continue
        return _pd.DataFrame([])
    
    groups: Dict[str, list] = {}
    weights_map: Dict[str, Dict[str, float]] = {}
    for idx in index_codes:
        df = await fetch_cons(idx)
        groups[idx] = df['symbol'].astype(str).tolist() if not df.empty else []
        if not df.empty and 'weight' in df.columns:
            w = _pd.to_numeric(df['weight'], errors='coerce')
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
            dq = _pd.DataFrame(env.data)
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