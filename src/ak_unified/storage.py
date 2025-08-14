from __future__ import annotations

import asyncio
import hashlib
import json
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import asyncpg  # type: ignore

_POOL: Optional[asyncpg.Pool] = None


def _dsn() -> Optional[str]:
    return os.environ.get("AKU_DB_DSN")


async def get_pool() -> Optional[asyncpg.Pool]:
    global _POOL
    dsn = _dsn()
    if not dsn:
        return None
    if _POOL is None:
        _POOL = await asyncpg.create_pool(dsn)
        await ensure_schema(_POOL)
    return _POOL


async def ensure_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            create table if not exists aku_cache (
              row_key text primary key,
              dataset_id text not null,
              symbol text,
              index_symbol text,
              board_code text,
              date date,
              datetime timestamptz,
              record jsonb not null,
              created_at timestamptz default now(),
              updated_at timestamptz default now()
            );
            create index if not exists idx_aku_cache_dataset_symbol_date on aku_cache(dataset_id, symbol, date);
            create index if not exists idx_aku_cache_dataset_indexsym_date on aku_cache(dataset_id, index_symbol, date);
            create index if not exists idx_aku_cache_dataset_board_datetime on aku_cache(dataset_id, board_code, datetime);
            """
        )


def _row_key(dataset_id: str, rec: Dict[str, Any]) -> str:
    parts = [dataset_id]
    for k in ("symbol", "index_symbol", "board_code"):
        v = rec.get(k)
        if v:
            parts.append(str(v))
            break
    v = rec.get("date") or rec.get("datetime")
    if v:
        parts.append(str(v))
    else:
        # fallback to content hash
        parts.append(hashlib.sha256(json.dumps(rec, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest())
    return "|".join(parts)


async def upsert_records(pool: asyncpg.Pool, dataset_id: str, records: Iterable[Dict[str, Any]]) -> None:
    if not records:
        return
    rows = []
    for r in records:
        rk = _row_key(dataset_id, r)
        rows.append((rk, dataset_id, r.get("symbol"), r.get("index_symbol"), r.get("board_code"), r.get("date"), r.get("datetime"), json.dumps(r, ensure_ascii=False)))
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            insert into aku_cache (row_key, dataset_id, symbol, index_symbol, board_code, date, datetime, record, updated_at)
            values ($1,$2,$3,$4,$5,$6,$7,$8, now())
            on conflict (row_key)
            do update set record = excluded.record, updated_at = now();
            """,
            rows,
        )


async def fetch_records(
    pool: asyncpg.Pool,
    dataset_id: str,
    *,
    symbol: Optional[str] = None,
    index_symbol: Optional[str] = None,
    board_code: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    time_field: Optional[str] = None,
) -> List[Dict[str, Any]]:
    where = ["dataset_id = $1"]
    args: List[Any] = [dataset_id]
    i = 2
    if symbol:
        where.append(f"symbol = ${i}")
        args.append(symbol)
        i += 1
    if index_symbol:
        where.append(f"index_symbol = ${i}")
        args.append(index_symbol)
        i += 1
    if board_code:
        where.append(f"board_code = ${i}")
        args.append(board_code)
        i += 1
    if time_field in ("date", "datetime") and start:
        where.append(f"{time_field} >= ${i}")
        args.append(start)
        i += 1
    if time_field in ("date", "datetime") and end:
        where.append(f"{time_field} <= ${i}")
        args.append(end)
        i += 1
    sql = f"select record from aku_cache where {' and '.join(where)} order by {time_field if time_field else 'updated_at'} asc"
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
    return [dict(r[0]) if isinstance(r[0], dict) else json.loads(r[0]) for r in rows]