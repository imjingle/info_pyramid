from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, List

from fastapi import FastAPI, Query, Body
from sse_starlette.sse import EventSourceResponse
from starlette.responses import JSONResponse

from .dispatcher import fetch_data, get_ohlcv, get_market_quote
from .dispatcher import get_ohlcva
from .registry import REGISTRY
from .adapters.qmt_adapter import test_qmt_import  # type: ignore
from .storage import get_pool as _get_pool, cache_stats as _cache_stats, purge_records as _purge_records  # type: ignore
from .storage import fetch_blob_snapshot as _blob_fetch, upsert_blob_snapshot as _blob_upsert, purge_blob as _blob_purge  # type: ignore

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
    use_cache: bool = Query(True),
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
    env = fetch_data(dataset_id, params, ak_function=ak_function, allow_fallback=allow_fallback, use_cache=use_cache)
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
    symbol: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    adjust: str = Query("none"),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
):
    # ohlcv uses the CN akshare dataset; for other adapters use /rpc/fetch with adapter
    env = get_ohlcv(symbol, start=start, end=end, adjust=adjust, ak_function=ak_function, allow_fallback=allow_fallback)
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
    env = get_ohlcva(symbol, start=start, end=end, adjust=adjust, ak_function=ak_function, allow_fallback=allow_fallback)
    return JSONResponse(content=env.model_dump(mode="json"), media_type="application/json")

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
    env = fetch_data("market.cn.board_aggregation.snapshot", params)
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
    env = fetch_data("market.cn.index_aggregation.snapshot", params)
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
    env = fetch_data("market.cn.aggregation.playback", params)
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
async def rpc_replay(dataset_id: str = Query(...), params: Dict[str, Any] = Query(...)) -> Dict[str, Any]:
    pool = await _get_pool()
    if pool is None:
        return {"ok": False, "error": "cache disabled"}
    res = await _blob_fetch(pool, dataset_id, params)
    if res is None:
        return {"ok": False, "error": "not found"}
    raw_obj, meta = res
    # Build an envelope-like response for convenience
    return {
        "ok": True,
        "dataset": dataset_id,
        "params": params,
        "ak_function": meta.get("ak_function"),
        "adapter": meta.get("adapter"),
        "timezone": meta.get("timezone"),
        "raw": raw_obj,
    }


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


@app.get("/topic/qmt/board")
async def topic_qmt_board(
    board_kind: str = Query("industry"),  # industry|concept
    boards: Optional[List[str]] = Query(None),  # list of board names to aggregate; if empty, aggregates all
    interval: float = 2.0,
    window_n: int = 10,
    topn: int = 5,
    bucket_sec: int = 60,
    history_buckets: int = 30,
    adapter_priority: Optional[List[str]] = Query(None),  # e.g., qmt,akshare
    include_percentiles: bool = True,
) -> EventSourceResponse:
    async def gen():
        status = test_qmt_import()
        if not status.get("ok") and (not adapter_priority or adapter_priority and adapter_priority[0] == 'qmt'):
            yield {"event": "error", "data": {"ok": False, "error": status.get("error"), "is_windows": status.get("is_windows")}}
            return
        ds = 'securities.board.cn.industry.qmt' if board_kind.lower().startswith('i') else 'securities.board.cn.concept.qmt'
        # allow fallback to akshare if qmt board dataset unsupported
        try:
            env = fetch_data(ds, {})
            df_list = env.data
        except Exception:
            df_list = []
        if not df_list:
            yield {"event": "update", "data": {"ok": True, "boards": [], "aggregates": []}}
            return
        import pandas as _pd
        from collections import deque
        from datetime import datetime, timedelta, timezone
        df = _pd.DataFrame(df_list)
        if boards:
            df = df[df['board_name'].astype(str).isin(boards)]
        groups = df.groupby('board_name')['symbol'].apply(list).to_dict()
        sym_set = sorted({s for lst in groups.values() for s in lst})

        # quotes fetcher with adapter fallback
        async def fetch_quotes(symbols: list[str]) -> _pd.DataFrame:
            pref = adapter_priority or ['qmt','akshare','qstock','efinance','adata']
            # qmt path
            if 'qmt' in pref:
                try:
                    from .adapters.qmt_adapter import fetch_realtime_quotes  # type: ignore
                    tag, qdf = fetch_realtime_quotes(symbols)
                    q = _pd.DataFrame(qdf)
                    if not q.empty:
                        return q
                except Exception:
                    pass
            # polling paths
            for adpt in ['akshare','qstock','efinance','adata']:
                if adpt not in pref:
                    continue
                try:
                    ds = 'securities.equity.cn.quote' if adpt == 'akshare' else f'securities.equity.cn.quote.{adpt}'
                    env = fetch_data(ds, {})
                    q = _pd.DataFrame(env.data)
                    if not q.empty:
                        q = q[q['symbol'].astype(str).isin(symbols)]
                        if not q.empty:
                            return q
                except Exception:
                    continue
            return _pd.DataFrame([])

        rolling = {b: {"avg_pct": deque(maxlen=window_n), "winners": deque(maxlen=window_n)} for b in groups.keys()}
        bucket_roll = {b: deque(maxlen=history_buckets) for b in groups.keys()}
        bucket_start = datetime.now(timezone.utc)
        try:
            # subscribe if qmt chosen
            if not adapter_priority or 'qmt' in adapter_priority:
                try:
                    from .adapters.qmt_adapter import subscribe_quotes  # type: ignore
                    subscribe_quotes(sym_set)
                except Exception:
                    pass
            failures = 0
            while True:
                try:
                    q = await fetch_quotes(sym_set)
                    if 'pct_change' not in q.columns and 'last' in q.columns and 'prev_close' in q.columns:
                        q['pct_change'] = ( _pd.to_numeric(q['last'], errors='coerce') / _pd.to_numeric(q['prev_close'], errors='coerce') - 1.0 ) * 100.0
                    q['amount'] = _pd.to_numeric(q.get('amount'), errors='coerce')
                    q['pct_change'] = _pd.to_numeric(q.get('pct_change'), errors='coerce')
                    if '量比' in q.columns:
                        q['volume_ratio'] = _pd.to_numeric(q['量比'], errors='coerce')
                    aggs = []
                    # compute cross-board percentiles if needed
                    board_stats = []
                    for b, syms in groups.items():
                        sub = q[q['symbol'].astype(str).isin(syms)] if not q.empty else _pd.DataFrame([])
                        if sub.empty:
                            board_stats.append((b, None, None))
                        else:
                            board_stats.append((b, float(sub['pct_change'].mean()), float(sub['amount'].sum()) if 'amount' in sub.columns else None))
                    # percentile helper
                    def percentile_rank(values, value):
                        s = _pd.Series([v for v in values if v is not None])
                        if s.empty or value is None:
                            return None
                        return float((s < value).mean())

                    for b, syms in groups.items():
                        sub = q[q['symbol'].astype(str).isin(syms)] if not q.empty else _pd.DataFrame([])
                        if sub.empty:
                            aggs.append({
                                "board_name": b,
                                "count": 0,
                                "winners_ratio": None,
                                "avg_pct_change": None,
                                "total_amount": None,
                                "advancers": 0,
                                "decliners": 0,
                                "unchanged": 0,
                                "limit_up": 0,
                                "limit_down": 0,
                                "top_amount": [],
                                "volume_ratio_avg": None,
                                "rolling_avg_pct_change": None,
                                "rolling_winners_ratio": None,
                                "bucket_ts": datetime.now(timezone.utc).isoformat(),
                                "pct_rank_vs_boards": None,
                            })
                            continue
                        winners = (sub['pct_change'] > 0).mean()
                        avg_pct = sub['pct_change'].mean()
                        total_amt = sub['amount'].sum() if 'amount' in sub.columns else None
                        advancers = int((sub['pct_change'] > 0).sum())
                        decliners = int((sub['pct_change'] < 0).sum())
                        unchanged = int((sub['pct_change'] == 0).sum()) if sub['pct_change'].notna().any() else 0
                        limit_up = int((sub['pct_change'] >= 9.8).sum())
                        limit_down = int((sub['pct_change'] <= -9.8).sum())
                        top = sub.sort_values('amount', ascending=False).head(topn) if 'amount' in sub.columns else _pd.DataFrame([])
                        top_list = top[['symbol', 'amount']].to_dict(orient='records') if not top.empty else []
                        vr_avg = sub['volume_ratio'].mean() if 'volume_ratio' in sub.columns else None
                        rolling[b]["avg_pct"].append(float(avg_pct) if avg_pct == avg_pct else 0.0)
                        rolling[b]["winners"].append(float(winners) if winners == winners else 0.0)
                        # minute-bucket aggregation
                        now = datetime.now(timezone.utc)
                        end_bucket = bucket_start + timedelta(seconds=bucket_sec)
                        if now >= end_bucket:
                            # finalize bucket metrics
                            bucket_roll[b].append({"ts": end_bucket.isoformat(), "avg_pct": float(_pd.Series(rolling[b]["avg_pct"]).mean()) if rolling[b]["avg_pct"] else None, "winners": float(_pd.Series(rolling[b]["winners"]).mean()) if rolling[b]["winners"] else None})
                        pct_rank = None
                        if include_percentiles:
                            vals = [v for (_, v, __) in board_stats]
                            pct_rank = percentile_rank(vals, float(avg_pct) if avg_pct == avg_pct else None)
                        aggs.append({
                            "board_name": b,
                            "count": int(len(sub)),
                            "winners_ratio": float(winners) if winners == winners else None,
                            "avg_pct_change": float(avg_pct) if avg_pct == avg_pct else None,
                            "total_amount": float(total_amt) if total_amt == total_amt else None,
                            "advancers": advancers,
                            "decliners": decliners,
                            "unchanged": unchanged,
                            "limit_up": limit_up,
                            "limit_down": limit_down,
                            "top_amount": top_list,
                            "volume_ratio_avg": float(vr_avg) if vr_avg == vr_avg else None,
                            "rolling_avg_pct_change": float(_pd.Series(rolling[b]["avg_pct"]).mean()) if rolling[b]["avg_pct"] else None,
                            "rolling_winners_ratio": float(_pd.Series(rolling[b]["winners"]).mean()) if rolling[b]["winners"] else None,
                            "bucket_ts": bucket_start.isoformat(),
                            "pct_rank_vs_boards": pct_rank,
                            "bucket_history": list(bucket_roll[b]),
                        })
                    if datetime.now(timezone.utc) >= (bucket_start + timedelta(seconds=bucket_sec)):
                        bucket_start = datetime.now(timezone.utc)
                    yield {"event": "update", "data": {"ok": True, "provider": "qmt", "boards": list(groups.keys()), "aggregates": aggs}}
                    failures = 0
                except Exception as e:  # noqa: BLE001
                    failures += 1
                    yield {"event": "error", "data": {"ok": False, "error": str(e), "retry": failures}}
                    if failures >= 5:
                        break
                await asyncio.sleep(interval)
        finally:
            try:
                from .adapters.qmt_adapter import unsubscribe_quotes  # type: ignore
                unsubscribe_quotes(sym_set)
            except Exception:
                pass
    return EventSourceResponse(gen())


@app.get("/topic/qmt/index")
async def topic_qmt_index(
    index_codes: List[str] = Query(...),  # e.g., 000300.SH
    interval: float = 2.0,
    window_n: int = 10,
    topn: int = 5,
    bucket_sec: int = 60,
    history_buckets: int = 30,
    adapter_priority: Optional[List[str]] = Query(None),
    include_percentiles: bool = True,
) -> EventSourceResponse:
    async def gen():
        status = test_qmt_import()
        if not status.get("ok") and (not adapter_priority or adapter_priority and adapter_priority[0] == 'qmt'):
            yield {"event": "error", "data": {"ok": False, "error": status.get("error"), "is_windows": status.get("is_windows")}}
            return
        import pandas as _pd
        from collections import deque
        from datetime import datetime, timedelta, timezone
        # Resolve constituents for each index
        groups: Dict[str, list] = {}
        for idx in index_codes:
            try:
                env = fetch_data('market.index.constituents.qmt', {"index_code": idx})
                df = _pd.DataFrame(env.data)
                groups[idx] = df['symbol'].astype(str).tolist() if not df.empty else []
            except Exception:
                groups[idx] = []
        sym_set = sorted({s for lst in groups.values() for s in lst})
        # quotes fetcher with fallback
        async def fetch_quotes(symbols: list[str]) -> _pd.DataFrame:
            pref = adapter_priority or ['qmt','akshare','qstock','efinance','adata']
            if 'qmt' in pref:
                try:
                    from .adapters.qmt_adapter import fetch_realtime_quotes  # type: ignore
                    tag, qdf = fetch_realtime_quotes(symbols)
                    q = _pd.DataFrame(qdf)
                    if not q.empty:
                        return q
                except Exception:
                    pass
            for adpt in ['akshare','qstock','efinance','adata']:
                if adpt not in pref:
                    continue
                try:
                    ds = 'securities.equity.cn.quote' if adpt == 'akshare' else f'securities.equity.cn.quote.{adpt}'
                    env = fetch_data(ds, {})
                    dq = _pd.DataFrame(env.data)
                    if not dq.empty:
                        dq = dq[dq['symbol'].astype(str).isin(symbols)]
                        if not dq.empty:
                            return dq
                except Exception:
                    continue
            return _pd.DataFrame([])
        # subscribe qmt
        if not adapter_priority or 'qmt' in adapter_priority:
            try:
                from .adapters.qmt_adapter import subscribe_quotes  # type: ignore
                subscribe_quotes(sym_set)
            except Exception:
                pass
        rolling = {idx: {"avg_pct": deque(maxlen=window_n), "winners": deque(maxlen=window_n)} for idx in groups.keys()}
        bucket_roll = {idx: deque(maxlen=history_buckets) for idx in groups.keys()}
        bucket_start = datetime.now(timezone.utc)
        failures = 0
        try:
            while True:
                try:
                    q = await fetch_quotes(sym_set)
                    if 'pct_change' not in q.columns and 'last' in q.columns and 'prev_close' in q.columns:
                        q['pct_change'] = ( _pd.to_numeric(q['last'], errors='coerce') / _pd.to_numeric(q['prev_close'], errors='coerce') - 1.0 ) * 100.0
                    q['amount'] = _pd.to_numeric(q.get('amount'), errors='coerce')
                    q['pct_change'] = _pd.to_numeric(q.get('pct_change'), errors='coerce')
                    aggs = []
                    # compute percentiles across indices
                    idx_vals = []
                    for idx, syms in groups.items():
                        sub = q[q['symbol'].astype(str).isin(syms)] if not q.empty else _pd.DataFrame([])
                        idx_vals.append((idx, float(sub['pct_change'].mean()) if not sub.empty else None))
                    def percentile_rank(values, value):
                        s = _pd.Series([v for v in values if v is not None])
                        if s.empty or value is None:
                            return None
                        return float((s < value).mean())
                    for idx, syms in groups.items():
                        sub = q[q['symbol'].astype(str).isin(syms)] if not q.empty else _pd.DataFrame([])
                        if sub.empty:
                            aggs.append({
                                "index_code": idx,
                                "count": 0,
                                "winners_ratio": None,
                                "avg_pct_change": None,
                                "total_amount": None,
                                "advancers": 0,
                                "decliners": 0,
                                "unchanged": 0,
                                "limit_up": 0,
                                "limit_down": 0,
                                "top_amount": [],
                                "rolling_avg_pct_change": None,
                                "rolling_winners_ratio": None,
                                "bucket_ts": datetime.now(timezone.utc).isoformat(),
                                "pct_rank_vs_indices": None,
                            })
                            continue
                        winners = (sub['pct_change'] > 0).mean()
                        avg_pct = sub['pct_change'].mean()
                        total_amt = sub['amount'].sum() if 'amount' in sub.columns else None
                        advancers = int((sub['pct_change'] > 0).sum())
                        decliners = int((sub['pct_change'] < 0).sum())
                        unchanged = int((sub['pct_change'] == 0).sum()) if sub['pct_change'].notna().any() else 0
                        limit_up = int((sub['pct_change'] >= 9.8).sum())
                        limit_down = int((sub['pct_change'] <= -9.8).sum())
                        top = sub.sort_values('amount', ascending=False).head(topn) if 'amount' in sub.columns else _pd.DataFrame([])
                        top_list = top[['symbol', 'amount']].to_dict(orient='records') if not top.empty else []
                        rolling[idx]["avg_pct"].append(float(avg_pct) if avg_pct == avg_pct else 0.0)
                        rolling[idx]["winners"].append(float(winners) if winners == winners else 0.0)
                        if datetime.now(timezone.utc) >= (bucket_start + timedelta(seconds=bucket_sec)):
                            bucket_roll[idx].append({"ts": (bucket_start + timedelta(seconds=bucket_sec)).isoformat(), "avg_pct": float(_pd.Series(rolling[idx]["avg_pct"]).mean()) if rolling[idx]["avg_pct"] else None, "winners": float(_pd.Series(rolling[idx]["winners"]).mean()) if rolling[idx]["winners"] else None})
                        pct_rank = percentile_rank([v for (_, v) in idx_vals], float(avg_pct) if avg_pct == avg_pct else None) if include_percentiles else None
                        aggs.append({
                            "index_code": idx,
                            "count": int(len(sub)),
                            "winners_ratio": float(winners) if winners == winners else None,
                            "avg_pct_change": float(avg_pct) if avg_pct == avg_pct else None,
                            "total_amount": float(total_amt) if total_amt == total_amt else None,
                            "advancers": advancers,
                            "decliners": decliners,
                            "unchanged": unchanged,
                            "limit_up": limit_up,
                            "limit_down": limit_down,
                            "top_amount": top_list,
                            "rolling_avg_pct_change": float(_pd.Series(rolling[idx]["avg_pct"]).mean()) if rolling[idx]["avg_pct"] else None,
                            "rolling_winners_ratio": float(_pd.Series(rolling[idx]["winners"]).mean()) if rolling[idx]["winners"] else None,
                            "bucket_ts": bucket_start.isoformat(),
                            "pct_rank_vs_indices": pct_rank,
                            "bucket_history": list(bucket_roll[idx]),
                        })
                    if datetime.now(timezone.utc) >= (bucket_start + timedelta(seconds=bucket_sec)):
                        bucket_start = datetime.now(timezone.utc)
                    yield {"event": "update", "data": {"ok": True, "provider": "qmt", "indices": list(groups.keys()), "aggregates": aggs}}
                    failures = 0
                except Exception as e:  # noqa: BLE001
                    failures += 1
                    yield {"event": "error", "data": {"ok": False, "error": str(e), "retry": failures}}
                    if failures >= 5:
                        break
                await asyncio.sleep(interval)
        finally:
            try:
                from .adapters.qmt_adapter import unsubscribe_quotes  # type: ignore
                unsubscribe_quotes(sym_set)
            except Exception:
                pass
    return EventSourceResponse(gen())

@app.get("/topic/board")
async def topic_board(
    board_kind: str = Query("industry"),  # industry|concept
    boards: List[str] = Query(...),
    interval: float = 2.0,
    window_n: int = 10,
    topn: int = 5,
    adapter_priority: Optional[List[str]] = Query(None),  # e.g., akshare,qstock,efinance,adata
    include_percentiles: bool = True,
    bucket_sec: int = 60,
    history_buckets: int = 30,
    weight_by: str = Query("none"),  # none|amount|weight|market_cap|float_market_cap
) -> EventSourceResponse:
    async def gen():
        import pandas as _pd
        from collections import deque
        from datetime import datetime, timezone, timedelta
        # resolve constituents per board using adapter priority
        def fetch_cons_one(b: str) -> _pd.DataFrame:
            for adpt in (adapter_priority or ['akshare','qstock','efinance','adata']):
                ds = 'securities.board.cn.industry.cons' if board_kind.lower().startswith('i') else 'securities.board.cn.concept.cons'
                ds = ds if adpt == 'akshare' else f"{ds}.{adpt}"
                try:
                    env = fetch_data(ds, {"board_code": b})
                    df = _pd.DataFrame(env.data)
                    if not df.empty and 'symbol' in df.columns:
                        return df[['symbol','weight']] if 'weight' in df.columns else df[['symbol']]
                except Exception:
                    continue
            return _pd.DataFrame([])

        groups: Dict[str, list] = {}
        weights_map: Dict[str, Dict[str, float]] = {}
        for b in boards:
            df = fetch_cons_one(b)
            groups[b] = df['symbol'].astype(str).tolist() if not df.empty else []
            if not df.empty and 'weight' in df.columns:
                w = _pd.to_numeric(df['weight'], errors='coerce')
                sym = df['symbol'].astype(str)
                weights_map[b] = {s: float(v) for s, v in zip(sym.tolist(), w.tolist()) if v == v}
            else:
                weights_map[b] = {}
        all_syms = sorted({s for lst in groups.values() for s in lst})

        async def fetch_quotes(symbols: list[str]) -> _pd.DataFrame:
            for adpt in (adapter_priority or ['akshare','qstock','efinance','adata']):
                ds = 'securities.equity.cn.quote' if adpt == 'akshare' else f'securities.equity.cn.quote.{adpt}'
                try:
                    env = fetch_data(ds, {})
                    q = _pd.DataFrame(env.data)
                    if not q.empty and 'symbol' in q.columns:
                        q = q[q['symbol'].astype(str).isin(symbols)]
                        if not q.empty:
                            return q
                except Exception:
                    continue
            return _pd.DataFrame([])

        rolling = {b: {"avg_pct": deque(maxlen=window_n), "winners": deque(maxlen=window_n)} for b in groups.keys()}
        bucket_roll = {b: deque(maxlen=history_buckets) for b in groups.keys()}
        bucket_start = datetime.now(timezone.utc)
        failures = 0
        while True:
            try:
                q = await fetch_quotes(all_syms)
                if 'pct_change' not in q.columns and 'last' in q.columns and 'prev_close' in q.columns:
                    q['pct_change'] = ( _pd.to_numeric(q['last'], errors='coerce') / _pd.to_numeric(q['prev_close'], errors='coerce') - 1.0 ) * 100.0
                q['amount'] = _pd.to_numeric(q.get('amount'), errors='coerce')
                q['pct_change'] = _pd.to_numeric(q.get('pct_change'), errors='coerce')
                aggs = []
                board_vals = []
                for b, syms in groups.items():
                    sub = q[q['symbol'].astype(str).isin(syms)] if not q.empty else _pd.DataFrame([])
                    if sub.empty:
                        aggs.append({
                            "board_name": b, "count": 0, "winners_ratio": None, "avg_pct_change": None,
                            "total_amount": None, "top_amount": [], "bucket_ts": datetime.now(timezone.utc).isoformat(),
                            "bucket_history": list(bucket_roll[b]),
                            "turnover_avg": None, "amplitude_avg": None, "volume_ratio_avg": None,
                        })
                        continue
                    winners = (sub['pct_change'] > 0).mean()
                    if weight_by == 'amount' and 'amount' in sub.columns:
                        w = _pd.to_numeric(sub['amount'], errors='coerce').fillna(0.0)
                        p = _pd.to_numeric(sub['pct_change'], errors='coerce').fillna(0.0)
                        avg_pct = (p * w).sum() / w.replace(0, _pd.NA).sum() if w.sum() > 0 else p.mean()
                    elif weight_by == 'weight' and weights_map.get(b):
                        sub = sub.copy()
                        sub['__w'] = sub['symbol'].astype(str).map(weights_map[b]).fillna(0.0)
                        w = _pd.to_numeric(sub['__w'], errors='coerce').fillna(0.0)
                        p = _pd.to_numeric(sub['pct_change'], errors='coerce').fillna(0.0)
                        avg_pct = (p * w).sum() / w.replace(0, _pd.NA).sum() if w.sum() > 0 else p.mean()
                    elif weight_by in ('market_cap','float_market_cap'):
                        # try multiple possible column names
                        cap_cols = ['market_cap','总市值','总市值(亿)','总市值-亿'] if weight_by == 'market_cap' else ['float_market_cap','流通市值','流通市值(亿)','流通市值-亿']
                        wcol = next((c for c in cap_cols if c in sub.columns), None)
                        if wcol:
                            w = _pd.to_numeric(sub[wcol], errors='coerce').fillna(0.0)
                            # normalize unit if in 亿
                            if any(u in wcol for u in ['(亿)','-亿']):
                                w = w * 1e8
                            p = _pd.to_numeric(sub['pct_change'], errors='coerce').fillna(0.0)
                            avg_pct = (p * w).sum() / w.replace(0, _pd.NA).sum() if w.sum() > 0 else p.mean()
                        else:
                            avg_pct = sub['pct_change'].mean()
                    else:
                        avg_pct = sub['pct_change'].mean()
                    total_amt = sub['amount'].sum() if 'amount' in sub.columns else None
                    top = sub.sort_values('amount', ascending=False).head(topn) if 'amount' in sub.columns else _pd.DataFrame([])
                    top_list = top[['symbol','amount']].to_dict(orient='records') if not top.empty else []
                    turnover_avg = _pd.to_numeric(sub.get('turnover_rate'), errors='coerce').mean() if 'turnover_rate' in sub.columns else None
                    amplitude_col = 'amplitude' if 'amplitude' in sub.columns else ('振幅' if '振幅' in sub.columns else None)
                    amplitude_avg = _pd.to_numeric(sub.get(amplitude_col), errors='coerce').mean() if amplitude_col else None
                    volume_ratio_avg = _pd.to_numeric(sub.get('量比'), errors='coerce').mean() if '量比' in sub.columns else (_pd.to_numeric(sub.get('volume_ratio'), errors='coerce').mean() if 'volume_ratio' in sub.columns else None)
                    rolling[b]["avg_pct"].append(float(avg_pct) if avg_pct == avg_pct else 0.0)
                    rolling[b]["winners"].append(float(winners) if winners == winners else 0.0)
                    now = datetime.now(timezone.utc)
                    end_bucket = bucket_start + timedelta(seconds=bucket_sec)
                    if now >= end_bucket:
                        bucket_roll[b].append({
                            "ts": end_bucket.isoformat(),
                            "avg_pct": float(_pd.Series(rolling[b]["avg_pct"]).mean()) if rolling[b]["avg_pct"] else None,
                            "winners": float(_pd.Series(rolling[b]["winners"]).mean()) if rolling[b]["winners"] else None,
                        })
                if datetime.now(timezone.utc) >= (bucket_start + timedelta(seconds=bucket_sec)):
                    bucket_start = datetime.now(timezone.utc)
                    board_vals.append((b, float(avg_pct) if avg_pct == avg_pct else None))
                    aggs.append({
                        "board_name": b,
                        "count": int(len(sub)),
                        "winners_ratio": float(winners) if winners == winners else None,
                        "avg_pct_change": float(avg_pct) if avg_pct == avg_pct else None,
                        "total_amount": float(total_amt) if total_amt == total_amt else None,
                        "top_amount": top_list,
                        "rolling_avg_pct_change": float(_pd.Series(rolling[b]["avg_pct"]).mean()) if rolling[b]["avg_pct"] else None,
                        "rolling_winners_ratio": float(_pd.Series(rolling[b]["winners"]).mean()) if rolling[b]["winners"] else None,
                        "bucket_ts": bucket_start.isoformat(),
                        "bucket_history": list(bucket_roll[b]),
                        "turnover_avg": float(turnover_avg) if turnover_avg == turnover_avg else None,
                        "amplitude_avg": float(amplitude_avg) if amplitude_avg == amplitude_avg else None,
                        "volume_ratio_avg": float(volume_ratio_avg) if volume_ratio_avg == volume_ratio_avg else None,
                    })
                if include_percentiles and aggs:
                    vals = [v for (_, v) in board_vals if v is not None]
                    for a in aggs:
                        v = a.get('avg_pct_change')
                        if v is not None and vals:
                            a['pct_rank_vs_boards'] = float((_pd.Series(vals) < v).mean())
                        else:
                            a['pct_rank_vs_boards'] = None
                yield {"event": "update", "data": {"ok": True, "boards": boards, "aggregates": aggs}}
                failures = 0
            except Exception as e:
                failures += 1
                yield {"event": "error", "data": {"ok": False, "error": str(e), "retry": failures}}
                if failures >= 5:
                    break
            await asyncio.sleep(interval)
    return EventSourceResponse(gen())

@app.get("/topic/index")
async def topic_index(
    index_codes: List[str] = Query(...),
    interval: float = 2.0,
    window_n: int = 10,
    topn: int = 5,
    adapter_priority: Optional[List[str]] = Query(None),
    include_percentiles: bool = True,
    bucket_sec: int = 60,
    history_buckets: int = 30,
    weight_by: str = Query("none"),  # none|amount|weight|market_cap|float_market_cap
) -> EventSourceResponse:
    async def gen():
        import pandas as _pd
        from collections import deque
        from datetime import datetime, timezone, timedelta
        groups: Dict[str, list] = {}
        def fetch_cons(idx: str) -> _pd.DataFrame:
            for adpt in (adapter_priority or ['akshare','qstock','efinance','adata']):
                ds = 'market.index.constituents' if adpt == 'akshare' else f'market.index.constituents.{adpt}'
                try:
                    env = fetch_data(ds, {"index_code": idx})
                    df = _pd.DataFrame(env.data)
                    if not df.empty and 'symbol' in df.columns:
                        return df[['symbol','weight']] if 'weight' in df.columns else df[['symbol']]
                except Exception:
                    continue
        
            return _pd.DataFrame([])

        weights_map: Dict[str, Dict[str, float]] = {}
        for idx in index_codes:
            df = fetch_cons(idx)
            groups[idx] = df['symbol'].astype(str).tolist() if not df.empty else []
            if not df.empty and 'weight' in df.columns:
                w = _pd.to_numeric(df['weight'], errors='coerce')
                sym = df['symbol'].astype(str)
                weights_map[idx] = {s: float(v) for s, v in zip(sym.tolist(), w.tolist()) if v == v}
            else:
                weights_map[idx] = {}
        all_syms = sorted({s for lst in groups.values() for s in lst})

        async def fetch_quotes(symbols: list[str]) -> _pd.DataFrame:
            for adpt in (adapter_priority or ['akshare','qstock','efinance','adata']):
                ds = 'securities.equity.cn.quote' if adpt == 'akshare' else f'securities.equity.cn.quote.{adpt}'
                try:
                    env = fetch_data(ds, {})
                    q = _pd.DataFrame(env.data)
                    if not q.empty and 'symbol' in q.columns:
                        q = q[q['symbol'].astype(str).isin(symbols)]
                        if not q.empty:
                            return q
                except Exception:
                    continue
            return _pd.DataFrame([])

        rolling = {idx: {"avg_pct": deque(maxlen=window_n), "winners": deque(maxlen=window_n)} for idx in groups.keys()}
        bucket_roll = {idx: deque(maxlen=history_buckets) for idx in groups.keys()}
        bucket_start = datetime.now(timezone.utc)
        failures = 0
        while True:
            try:
                q = await fetch_quotes(all_syms)
                if 'pct_change' not in q.columns and 'last' in q.columns and 'prev_close' in q.columns:
                    q['pct_change'] = ( _pd.to_numeric(q['last'], errors='coerce') / _pd.to_numeric(q['prev_close'], errors='coerce') - 1.0 ) * 100.0
                q['amount'] = _pd.to_numeric(q.get('amount'), errors='coerce')
                q['pct_change'] = _pd.to_numeric(q.get('pct_change'), errors='coerce')
                aggs = []
                idx_vals = []
                for idx, syms in groups.items():
                    sub = q[q['symbol'].astype(str).isin(syms)] if not q.empty else _pd.DataFrame([])
                    if sub.empty:
                        aggs.append({"index_code": idx, "count": 0, "winners_ratio": None, "avg_pct_change": None, "total_amount": None, "top_amount": [], "bucket_ts": bucket_start.isoformat(), "bucket_history": list(bucket_roll[idx])})
                        continue
                    winners = (sub['pct_change'] > 0).mean()
                    if weight_by == 'amount' and 'amount' in sub.columns:
                        w = _pd.to_numeric(sub['amount'], errors='coerce').fillna(0.0)
                        p = _pd.to_numeric(sub['pct_change'], errors='coerce').fillna(0.0)
                        avg_pct = (p * w).sum() / w.replace(0, _pd.NA).sum() if w.sum() > 0 else p.mean()
                    elif weight_by == 'weight' and weights_map.get(idx):
                        sub = sub.copy()
                        sub['__w'] = sub['symbol'].astype(str).map(weights_map[idx]).fillna(0.0)
                        w = _pd.to_numeric(sub['__w'], errors='coerce').fillna(0.0)
                        p = _pd.to_numeric(sub['pct_change'], errors='coerce').fillna(0.0)
                        avg_pct = (p * w).sum() / w.replace(0, _pd.NA).sum() if w.sum() > 0 else p.mean()
                    elif weight_by in ('market_cap','float_market_cap'):
                        cap_cols = ['market_cap','总市值','总市值(亿)','总市值-亿'] if weight_by == 'market_cap' else ['float_market_cap','流通市值','流通市值(亿)','流通市值-亿']
                        wcol = next((c for c in cap_cols if c in sub.columns), None)
                        if wcol:
                            w = _pd.to_numeric(sub[wcol], errors='coerce').fillna(0.0)
                            if any(u in wcol for u in ['(亿)','-亿']):
                                w = w * 1e8
                            p = _pd.to_numeric(sub['pct_change'], errors='coerce').fillna(0.0)
                            avg_pct = (p * w).sum() / w.replace(0, _pd.NA).sum() if w.sum() > 0 else p.mean()
                        else:
                            avg_pct = sub['pct_change'].mean()
                    else:
                        avg_pct = sub['pct_change'].mean()
                    total_amt = sub['amount'].sum() if 'amount' in sub.columns else None
                    top = sub.sort_values('amount', ascending=False).head(topn) if 'amount' in sub.columns else _pd.DataFrame([])
                    top_list = top[['symbol','amount']].to_dict(orient='records') if not top.empty else []
                    turnover_avg = _pd.to_numeric(sub.get('turnover_rate'), errors='coerce').mean() if 'turnover_rate' in sub.columns else None
                    amplitude_col = 'amplitude' if 'amplitude' in sub.columns else ('振幅' if '振幅' in sub.columns else None)
                    amplitude_avg = _pd.to_numeric(sub.get(amplitude_col), errors='coerce').mean() if amplitude_col else None
                    volume_ratio_avg = _pd.to_numeric(sub.get('量比'), errors='coerce').mean() if '量比' in sub.columns else (_pd.to_numeric(sub.get('volume_ratio'), errors='coerce').mean() if 'volume_ratio' in sub.columns else None)
                    rolling[idx]["avg_pct"].append(float(avg_pct) if avg_pct == avg_pct else 0.0)
                    rolling[idx]["winners"].append(float(winners) if winners == winners else 0.0)
                    now = datetime.now(timezone.utc)
                    end_bucket = bucket_start + timedelta(seconds=bucket_sec)
                    if now >= end_bucket:
                        bucket_roll[idx].append({
                            "ts": end_bucket.isoformat(),
                            "avg_pct": float(_pd.Series(rolling[idx]["avg_pct"]).mean()) if rolling[idx]["avg_pct"] else None,
                            "winners": float(_pd.Series(rolling[idx]["winners"]).mean()) if rolling[idx]["winners"] else None,
                        })
                if datetime.now(timezone.utc) >= (bucket_start + timedelta(seconds=bucket_sec)):
                    bucket_start = datetime.now(timezone.utc)
                    idx_vals.append((idx, float(avg_pct) if avg_pct == avg_pct else None))
                    aggs.append({
                        "index_code": idx,
                        "count": int(len(sub)),
                        "winners_ratio": float(winners) if winners == winners else None,
                        "avg_pct_change": float(avg_pct) if avg_pct == avg_pct else None,
                        "total_amount": float(total_amt) if total_amt == total_amt else None,
                        "top_amount": top_list,
                        "rolling_avg_pct_change": float(_pd.Series(rolling[idx]["avg_pct"]).mean()) if rolling[idx]["avg_pct"] else None,
                        "rolling_winners_ratio": float(_pd.Series(rolling[idx]["winners"]).mean()) if rolling[idx]["winners"] else None,
                        "bucket_ts": bucket_start.isoformat(),
                        "bucket_history": list(bucket_roll[idx]),
                        "turnover_avg": float(turnover_avg) if turnover_avg == turnover_avg else None,
                        "amplitude_avg": float(amplitude_avg) if amplitude_avg == amplitude_avg else None,
                        "volume_ratio_avg": float(volume_ratio_avg) if volume_ratio_avg == volume_ratio_avg else None,
                    })
                if include_percentiles and aggs:
                    vals = [v for (_, v) in idx_vals if v is not None]
                    for a in aggs:
                        v = a.get('avg_pct_change')
                        if v is not None and vals:
                            a['pct_rank_vs_indices'] = float((_pd.Series(vals) < v).mean())
                        else:
                            a['pct_rank_vs_indices'] = None
                yield {"event": "update", "data": {"ok": True, "indices": index_codes, "aggregates": aggs}}
                failures = 0
            except Exception as e:
                failures += 1
                yield {"event": "error", "data": {"ok": False, "error": str(e), "retry": failures}}
                if failures >= 5:
                    break
            await asyncio.sleep(interval)
    return EventSourceResponse(gen())