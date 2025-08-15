from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from typing import Any, Dict, Optional, Set

import aiofiles
import pandas as pd

from ..storage import get_pool, fetch_records, upsert_records
from ..logging import logger


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
            async with aiofiles.open(output, 'w', encoding='utf-8') as f:
                while True:
                    rows = await cur.fetch(chunk_size)
                    if not rows:
                        break
                    for ds, rec in rows:
                        obj = rec if isinstance(rec, dict) else json.loads(rec)
                        obj = normalize_record(obj, rename_map=rename_map, keep_fields=keep_fields, drop_fields=drop_fields)
                        await f.write(json.dumps({"dataset_id": ds, "record": obj}, ensure_ascii=False) + "\n")
                        count += 1
        except AttributeError:
            offset = 0
            async with aiofiles.open(output, 'w', encoding='utf-8') as f:
                while True:
                    rows = await conn.fetch(sql + f" limit {chunk_size} offset {offset}", *args)
                    if not rows:
                        break
                    for ds, rec in rows:
                        obj = rec if isinstance(rec, dict) else json.loads(rec)
                        obj = normalize_record(obj, rename_map=rename_map, keep_fields=keep_fields, drop_fields=drop_fields)
                        await f.write(json.dumps({"dataset_id": ds, "record": obj}, ensure_ascii=False) + "\n")
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
    batch: Dict[str, list] = {}
    total = 0
    async with aiofiles.open(input_path, 'r', encoding='utf-8') as f:
        async for line in f:
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
                        await upsert_records(pool, ds_id, recs)
                    batch.clear()
            except Exception:
                continue
    if batch:
        for ds_id, recs in batch.items():
            await upsert_records(pool, ds_id, recs)
    return total


async def export_cache_to_csv(
    dataset_id: str,
    output: str,
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """Export cache data to CSV file asynchronously."""
    try:
        # Get database pool
        pool = await get_pool()
        if not pool:
            return {"success": False, "error": "Database not available"}
        
        # Build query parameters
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        
        # Fetch data
        records = await fetch_records(pool, dataset_id, params)
        
        if not records:
            return {"success": False, "error": "No data found"}
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Apply limit if specified
        if limit and len(df) > limit:
            df = df.head(limit)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output), exist_ok=True)
        
        # Write to CSV asynchronously
        async with aiofiles.open(output, 'w', encoding='utf-8') as f:
            await f.write(df.to_csv(index=False))
        
        return {
            "success": True,
            "output_file": output,
            "records_exported": len(df),
            "total_records": len(records)
        }
        
    except Exception as e:
        logger.error(f"Failed to export cache to CSV: {e}")
        return {"success": False, "error": str(e)}


async def export_cache_to_json(
    dataset_id: str,
    output: str,
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: Optional[int] = None,
    pretty: bool = True
) -> Dict[str, Any]:
    """Export cache data to JSON file asynchronously."""
    try:
        # Get database pool
        pool = await get_pool()
        if not pool:
            return {"success": False, "error": "Database not available"}
        
        # Build query parameters
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        
        # Fetch data
        records = await fetch_records(pool, dataset_id, params)
        
        if not records:
            return {"success": False, "error": "No data found"}
        
        # Apply limit if specified
        if limit and len(records) > limit:
            records = records[:limit]
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output), exist_ok=True)
        
        # Write to JSON asynchronously
        async with aiofiles.open(output, 'w', encoding='utf-8') as f:
            if pretty:
                await f.write(json.dumps(records, indent=2, ensure_ascii=False, default=str))
            else:
                await f.write(json.dumps(records, ensure_ascii=False, default=str))
        
        return {
            "success": True,
            "output_file": output,
            "records_exported": len(records),
            "total_records": len(records)
        }
        
    except Exception as e:
        logger.error(f"Failed to export cache to JSON: {e}")
        return {"success": False, "error": str(e)}


async def import_cache_from_json(
    input_path: str,
    dataset_id: str,
    overwrite: bool = False
) -> Dict[str, Any]:
    """Import cache data from JSON file asynchronously."""
    try:
        # Check if input file exists
        if not os.path.exists(input_path):
            return {"success": False, "error": f"Input file not found: {input_path}"}
        
        # Read JSON file asynchronously
        async with aiofiles.open(input_path, 'r', encoding='utf-8') as f:
            content = await f.read()
            data = json.loads(content)
        
        if not isinstance(data, list):
            return {"success": False, "error": "Invalid JSON format: expected list of records"}
        
        if not data:
            return {"success": False, "error": "No data in JSON file"}
        
        # Get database pool
        pool = await get_pool()
        if not pool:
            return {"success": False, "error": "Database not available"}
        
        # Upsert data
        await upsert_records(pool, dataset_id, data)
        
        return {
            "success": True,
            "dataset_id": dataset_id,
            "records_imported": len(data),
            "overwrite": overwrite
        }
        
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"Invalid JSON: {e}"}
    except Exception as e:
        logger.error(f"Failed to import cache from JSON: {e}")
        return {"success": False, "error": str(e)}


async def cache_stats_summary() -> Dict[str, Any]:
    """Get summary statistics of cache data."""
    try:
        # Get database pool
        pool = await get_pool()
        if not pool:
            return {"success": False, "error": "Database not available"}
        
        # This would need to be implemented in storage module
        # For now, return a placeholder
        return {
            "success": True,
            "total_datasets": 0,
            "total_records": 0,
            "cache_size_mb": 0
        }
        
    except Exception as e:
        logger.error(f"Failed to get cache stats: {e}")
        return {"success": False, "error": str(e)}


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