from __future__ import annotations

import argparse
import asyncio
import json
import math
from typing import Any, Dict, Optional, Set

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


def normalize_record(
    rec: Dict[str, Any],
    *,
    rename_map: Optional[Dict[str, str]] = None,
    keep_fields: Optional[Set[str]] = None,
    drop_fields: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    items = list(rec.items())
    # optional rename first
    if rename_map:
        items = [(rename_map.get(str(k), str(k)), v) for k, v in items]
    for k, v in items:
        key = str(k)
        if drop_fields and key in drop_fields:
            continue
        if keep_fields is not None and key not in keep_fields:
            continue
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
        if key in ('symbol', 'index_symbol', 'board_code'):
            if isinstance(v, str):
                out[key] = v.strip().upper()
            else:
                out[key] = v
            continue
        if key in _NUMERIC_FIELDS:
            f = _to_float(v)
            out[key] = f
            continue
        out[key] = v
    return out


async def export_cache(
    output: str,
    dataset_prefix: Optional[str] = None,
    *,
    time_field: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    chunk_size: int = 5000,
    rename_map: Optional[Dict[str, str]] = None,
    keep_fields: Optional[Set[str]] = None,
    drop_fields: Optional[Set[str]] = None,
) -> int:
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
        try:
            cur = await conn.cursor(sql, *args)
            with open(output, 'w', encoding='utf-8') as f:
                while True:
                    rows = await cur.fetch(chunk_size)
                    if not rows:
                        break
                    for ds, rec in rows:
                        obj = rec if isinstance(rec, dict) else json.loads(rec)
                        obj = normalize_record(obj, rename_map=rename_map, keep_fields=keep_fields, drop_fields=drop_fields)
                        f.write(json.dumps({"dataset_id": ds, "record": obj}, ensure_ascii=False) + "\n")
                        count += 1
        except AttributeError:
            offset = 0
            with open(output, 'w', encoding='utf-8') as f:
                while True:
                    rows = await conn.fetch(sql + f" limit {chunk_size} offset {offset}", *args)
                    if not rows:
                        break
                    for ds, rec in rows:
                        obj = rec if isinstance(rec, dict) else json.loads(rec)
                        obj = normalize_record(obj, rename_map=rename_map, keep_fields=keep_fields, drop_fields=drop_fields)
                        f.write(json.dumps({"dataset_id": ds, "record": obj}, ensure_ascii=False) + "\n")
                        count += 1
                    offset += chunk_size
    return count


async def import_cache(
    input_path: str,
    dataset_prefix: Optional[str] = None,
    *,
    batch_size: int = 1000,
    rename_map: Optional[Dict[str, str]] = None,
    keep_fields: Optional[Set[str]] = None,
    drop_fields: Optional[Set[str]] = None,
) -> int:
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
                norm = normalize_record(rec, rename_map=rename_map, keep_fields=keep_fields, drop_fields=drop_fields)
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


def _parse_json_map(s: Optional[str]) -> Optional[Dict[str, str]]:
    if not s:
        return None
    try:
        m = json.loads(s)
        if isinstance(m, dict):
            return {str(k): str(v) for k, v in m.items()}
    except Exception:
        return None
    return None


def _parse_csv_set(s: Optional[str]) -> Optional[Set[str]]:
    if not s:
        return None
    return {x.strip() for x in s.split(',') if x.strip()}


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
    p_exp.add_argument('--rename-map-json', default=None, help='JSON mapping to rename fields during export')
    p_exp.add_argument('--keep-fields', default=None, help='CSV of fields to keep (others dropped)')
    p_exp.add_argument('--drop-fields', default=None, help='CSV of fields to drop')

    p_imp = sub.add_parser('import', help='Import cache from NDJSON')
    p_imp.add_argument('-i', '--input', required=True, help='Input NDJSON file')
    p_imp.add_argument('--dataset-prefix', default=None, help='Filter dataset_id by prefix while importing')
    p_imp.add_argument('--batch-size', type=int, default=1000, help='Upsert batch size')
    p_imp.add_argument('--rename-map-json', default=None, help='JSON mapping to rename fields during import')
    p_imp.add_argument('--keep-fields', default=None, help='CSV of fields to keep (others dropped)')
    p_imp.add_argument('--drop-fields', default=None, help='CSV of fields to drop')

    args = parser.parse_args()
    if args.cmd == 'export':
        cnt = asyncio.run(export_cache(
            args.output,
            args.dataset_prefix,
            time_field=args.time_field,
            start=args.start,
            end=args.end,
            chunk_size=args.chunk_size,
            rename_map=_parse_json_map(args.rename_map_json),
            keep_fields=_parse_csv_set(args.keep_fields),
            drop_fields=_parse_csv_set(args.drop_fields),
        ))
        print(f"exported {cnt} rows")
    elif args.cmd == 'import':
        cnt = asyncio.run(import_cache(
            args.input,
            args.dataset_prefix,
            batch_size=args.batch_size,
            rename_map=_parse_json_map(args.rename_map_json),
            keep_fields=_parse_csv_set(args.keep_fields),
            drop_fields=_parse_csv_set(args.drop_fields),
        ))
        print(f"imported {cnt} rows")


if __name__ == '__main__':
    main()