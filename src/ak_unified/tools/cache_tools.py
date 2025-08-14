from __future__ import annotations

import argparse
import asyncio
import json
import math
from typing import Any, Dict, Optional

import pandas as pd

from ..storage import get_pool


_NUMERIC_FIELDS = {
    'open','high','low','close','last','prev_close','change','pct_change',
    'volume','amount','turnover_rate','pe','pe_ttm','pb','nav','acc_nav',
    'daily_return','yield','yield_','settlement','open_interest','iv','delta',
    'gamma','vega','theta','rho','weight'
}


def _to_date_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        ts = pd.to_datetime(v)
        return ts.strftime('%Y-%m-%d')
    except Exception:
        return None


def _to_datetime_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        ts = pd.to_datetime(v)
        # keep ISO8601, naive or with tz if present
        return ts.isoformat()
    except Exception:
        return None


def _to_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
        if math.isfinite(f):
            return f
        return None
    except Exception:
        return None


def normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in rec.items():
        key = str(k)
        if key == 'date':
            ds = _to_date_str(v)
            if ds is not None:
                out['date'] = ds
            continue
        if key == 'datetime':
            ds = _to_datetime_str(v)
            if ds is not None:
                out['datetime'] = ds
            continue
        if key == 'symbol' or key == 'index_symbol' or key == 'board_code':
            if isinstance(v, str):
                out[key] = v.strip().upper()
            else:
                out[key] = v
            continue
        if key in _NUMERIC_FIELDS:
            f = _to_float(v)
            if f is not None:
                out[key] = f
            else:
                # keep None for invalid numerics
                out[key] = None
            continue
        # passthrough other fields
        out[key] = v
    return out


async def export_cache(output: str, dataset_prefix: Optional[str] = None, *, time_field: Optional[str] = None, start: Optional[str] = None, end: Optional[str] = None, chunk_size: int = 5000) -> int:
    pool = await get_pool()
    if pool is None:
        raise RuntimeError("AKU_DB_DSN not configured")
    where = []
    args = []
    if dataset_prefix:
        where.append("dataset_id like $1")
        args.append(dataset_prefix + '%')
    if time_field in ("date", "datetime") and start:
        where.append(f"{time_field} >= ${len(args)+1}")
        args.append(start)
    if time_field in ("date", "datetime") and end:
        where.append(f"{time_field} <= ${len(args)+1}")
        args.append(end)
    where_sql = (" where " + " and ".join(where)) if where else ""
    sql = f"select dataset_id, record from aku_cache{where_sql} order by dataset_id"
    count = 0
    async with pool.acquire() as conn:
        # asyncpg cursor streaming
        try:
            cur = await conn.cursor(sql, *args)
            with open(output, 'w', encoding='utf-8') as f:
                while True:
                    rows = await cur.fetch(chunk_size)
                    if not rows:
                        break
                    for ds, rec in rows:
                        obj = rec if isinstance(rec, dict) else json.loads(rec)
                        obj = normalize_record(obj)
                        f.write(json.dumps({"dataset_id": ds, "record": obj}, ensure_ascii=False) + "\n")
                        count += 1
        except AttributeError:
            # fallback to manual paging
            offset = 0
            with open(output, 'w', encoding='utf-8') as f:
                while True:
                    rows = await conn.fetch(sql + f" limit {chunk_size} offset {offset}", *args)
                    if not rows:
                        break
                    for ds, rec in rows:
                        obj = rec if isinstance(rec, dict) else json.loads(rec)
                        obj = normalize_record(obj)
                        f.write(json.dumps({"dataset_id": ds, "record": obj}, ensure_ascii=False) + "\n")
                        count += 1
                    offset += chunk_size
    return count


async def import_cache(input_path: str, dataset_prefix: Optional[str] = None, *, batch_size: int = 1000) -> int:
    pool = await get_pool()
    if pool is None:
        raise RuntimeError("AKU_DB_DSN not configured")
    from ..storage import upsert_records as upsert
    batch: Dict[str, list] = {}
    total = 0
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                ds = obj.get('dataset_id')
                rec = obj.get('record')
                if not isinstance(rec, dict) or not isinstance(ds, str):
                    continue
                if dataset_prefix and not ds.startswith(dataset_prefix):
                    continue
                norm = normalize_record(rec)
                # basic validation: require either date or datetime key for time series
                if 'date' not in norm and 'datetime' not in norm:
                    # allow import but consistent hashing will fallback; optionally skip
                    pass
                batch.setdefault(ds, []).append(norm)
                total += 1
                if sum(len(v) for v in batch.values()) >= batch_size:
                    for ds_id, recs in batch.items():
                        await upsert(pool, ds_id, recs)
                    batch.clear()
            except Exception:
                continue
    if batch:
        for ds_id, recs in batch.items():
            await upsert(pool, ds_id, recs)
    return total


def main():
    parser = argparse.ArgumentParser(description="AK Unified cache export/import")
    sub = parser.add_subparsers(dest='cmd', required=True)
    p_exp = sub.add_parser('export', help='Export cache to NDJSON')
    p_exp.add_argument('-o', '--output', required=True, help='Output file path')
    p_exp.add_argument('--dataset-prefix', default=None, help='Filter dataset_id by prefix')
    p_exp.add_argument('--time-field', default=None, choices=['date','datetime'], help='Filter by time field')
    p_exp.add_argument('--start', default=None, help='Start bound for time field')
    p_exp.add_argument('--end', default=None, help='End bound for time field')
    p_exp.add_argument('--chunk-size', type=int, default=5000, help='Export chunk size')

    p_imp = sub.add_parser('import', help='Import cache from NDJSON')
    p_imp.add_argument('-i', '--input', required=True, help='Input NDJSON file')
    p_imp.add_argument('--dataset-prefix', default=None, help='Filter dataset_id by prefix while importing')
    p_imp.add_argument('--batch-size', type=int, default=1000, help='Upsert batch size')

    args = parser.parse_args()
    if args.cmd == 'export':
        cnt = asyncio.run(export_cache(args.output, args.dataset_prefix, time_field=args.time_field, start=args.start, end=args.end, chunk_size=args.chunk_size))
        print(f"exported {cnt} rows")
    elif args.cmd == 'import':
        cnt = asyncio.run(import_cache(args.input, args.dataset_prefix, batch_size=args.batch_size))
        print(f"imported {cnt} rows")


if __name__ == '__main__':
    main()