from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query
from sse_starlette.sse import EventSourceResponse

from .dispatcher import fetch_data, get_ohlcv, get_market_quote

app = FastAPI(title="AK Unified API", version="0.1.0")


@app.get("/rpc/fetch")
async def rpc_fetch(
    dataset_id: str = Query(...),
    ak_function: Optional[str] = Query(None),
    allow_fallback: bool = Query(False),
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    freq: Optional[str] = None,
    adjust: Optional[str] = None,
    board_code: Optional[str] = None,
    index_code: Optional[str] = None,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    for k, v in {
        "symbol": symbol,
        "start": start,
        "end": end,
        "freq": freq,
        "adjust": adjust,
        "board_code": board_code,
        "index_code": index_code,
    }.items():
        if v is not None:
            params[k] = v
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
    symbol: Optional[str] = None,
    freq: Optional[str] = None,
    adjust: Optional[str] = None,
) -> EventSourceResponse:
    params: Dict[str, Any] = {}
    for k, v in {"symbol": symbol, "freq": freq, "adjust": adjust}.items():
        if v is not None:
            params[k] = v
    generator = _polling_generator(dataset_id, params, ak_function, interval)
    return EventSourceResponse(generator)