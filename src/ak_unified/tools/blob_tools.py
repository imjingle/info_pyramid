from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
from typing import Any, Dict, List, Optional

import aiofiles
import pandas as pd

from ..storage import get_pool, fetch_blob_snapshot, upsert_blob_snapshot
from ..logging import logger


async def export_blob_to_file(
    dataset_id: str,
    params: Dict[str, Any],
    output: str,
    format: str = "json"
) -> Dict[str, Any]:
    """Export blob data to file asynchronously."""
    try:
        # Get database pool
        pool = await get_pool()
        if not pool:
            return {"success": False, "error": "Database not available"}
        
        # Fetch blob data
        result = await fetch_blob_snapshot(pool, dataset_id, params)
        if not result:
            return {"success": False, "error": "No blob data found"}
        
        raw_data, meta = result
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output), exist_ok=True)
        
        # Write data based on format
        if format.lower() == "json":
            async with aiofiles.open(output, 'w', encoding='utf-8') as f:
                await f.write(json.dumps({
                    "dataset_id": dataset_id,
                    "params": params,
                    "meta": meta,
                    "data": raw_data
                }, indent=2, ensure_ascii=False, default=str))
        elif format.lower() == "raw":
            async with aiofiles.open(output, 'wb') as f:
                await f.write(raw_data)
        else:
            return {"success": False, "error": f"Unsupported format: {format}"}
        
        return {
            "success": True,
            "output_file": output,
            "format": format,
            "data_size": len(raw_data) if isinstance(raw_data, bytes) else len(str(raw_data))
        }
        
    except Exception as e:
        logger.error(f"Failed to export blob to file: {e}")
        return {"success": False, "error": str(e)}


async def import_blob_from_file(
    input_path: str,
    dataset_id: str,
    params: Dict[str, Any],
    encoding: str = "raw"
) -> Dict[str, Any]:
    """Import blob data from file asynchronously."""
    try:
        # Check if input file exists
        if not os.path.exists(input_path):
            return {"success": False, "error": f"Input file not found: {input_path}"}
        
        # Get database pool
        pool = await get_pool()
        if not pool:
            return {"success": False, "error": "Database not available"}
        
        # Read file based on encoding
        if encoding.lower() == "raw":
            async with aiofiles.open(input_path, 'rb') as f:
                raw_data = await f.read()
        else:
            async with aiofiles.open(input_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                raw_data = content.encode('utf-8')
        
        # Upsert blob data
        await upsert_blob_snapshot(pool, dataset_id, params, raw_data, encoding)
        
        return {
            "success": True,
            "dataset_id": dataset_id,
            "input_file": input_path,
            "data_size": len(raw_data),
            "encoding": encoding
        }
        
    except Exception as e:
        logger.error(f"Failed to import blob from file: {e}")
        return {"success": False, "error": str(e)}


async def batch_export_blobs(
    dataset_ids: List[str],
    output_dir: str,
    format: str = "json"
) -> Dict[str, Any]:
    """Batch export multiple blob datasets to files."""
    try:
        # Get database pool
        pool = await get_pool()
        if not pool:
            return {"success": False, "error": "Database not available"}
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        results = []
        total_size = 0
        
        for dataset_id in dataset_ids:
            try:
                # Export each dataset
                output_file = os.path.join(output_dir, f"{dataset_id.replace('.', '_')}.{format}")
                result = await export_blob_to_file(dataset_id, {}, output_file, format)
                
                if result["success"]:
                    results.append({
                        "dataset_id": dataset_id,
                        "output_file": output_file,
                        "data_size": result.get("data_size", 0)
                    })
                    total_size += result.get("data_size", 0)
                else:
                    results.append({
                        "dataset_id": dataset_id,
                        "error": result.get("error", "Unknown error")
                    })
                    
            except Exception as e:
                results.append({
                    "dataset_id": dataset_id,
                    "error": str(e)
                })
        
        return {
            "success": True,
            "total_datasets": len(dataset_ids),
            "successful_exports": len([r for r in results if "error" not in r]),
            "failed_exports": len([r for r in results if "error" in r]),
            "total_size": total_size,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Failed to batch export blobs: {e}")
        return {"success": False, "error": str(e)}


async def blob_stats_summary() -> Dict[str, Any]:
    """Get summary statistics of blob data."""
    try:
        # Get database pool
        pool = await get_pool()
        if not pool:
            return {"success": False, "error": "Database not available"}
        
        # This would need to be implemented in storage module
        # For now, return a placeholder
        return {
            "success": True,
            "total_blobs": 0,
            "total_size_mb": 0,
            "oldest_blob": None,
            "newest_blob": None
        }
        
    except Exception as e:
        logger.error(f"Failed to get blob stats: {e}")
        return {"success": False, "error": str(e)}


async def export_blobs(
    output: str,
    *,
    dataset_prefix: Optional[str] = None,
    updated_after: Optional[str] = None,
    updated_before: Optional[str] = None,
    chunk_size: int = 2000,
) -> int:
    pool = await get_pool()
    if pool is None:
        raise RuntimeError("AKU_DB_DSN not configured")
    where = []
    args = []
    if dataset_prefix:
        where.append("dataset_id like $1")
        args.append(dataset_prefix + '%')
    if updated_after:
        where.append(f"updated_at >= ${len(args)+1}")
        args.append(updated_after)
    if updated_before:
        where.append(f"updated_at <= ${len(args)+1}")
        args.append(updated_before)
    where_sql = (" where " + " and ".join(where)) if where else ""
    sql = f"select dataset_id, params, ak_function, adapter, timezone, raw_data, encoding from aku_cache_blob{where_sql} order by updated_at desc"
    count = 0
    async with pool.acquire() as conn:
        try:
            cur = await conn.cursor(sql, *args)
            with open(output, 'w', encoding='utf-8') as f:
                while True:
                    rows = await cur.fetch(chunk_size)
                    if not rows:
                        break
                    for ds, params_json, akf, adp, tz, raw, enc in rows:
                        line = {
                            "dataset_id": ds,
                            "params": json.loads(params_json) if isinstance(params_json, str) else (params_json or {}),
                            "ak_function": akf,
                            "adapter": adp,
                            "timezone": tz,
                            "encoding": enc or 'raw',
                            "raw_b64": base64.b64encode(bytes(raw)).decode('ascii'),
                        }
                        f.write(json.dumps(line, ensure_ascii=False) + "\n")
                        count += 1
        except AttributeError:
            offset = 0
            with open(output, 'w', encoding='utf-8') as f:
                while True:
                    rows = await conn.fetch(sql + f" limit {chunk_size} offset {offset}", *args)
                    if not rows:
                        break
                    for ds, params_json, akf, adp, tz, raw, enc in rows:
                        line = {
                            "dataset_id": ds,
                            "params": json.loads(params_json) if isinstance(params_json, str) else (params_json or {}),
                            "ak_function": akf,
                            "adapter": adp,
                            "timezone": tz,
                            "encoding": enc or 'raw',
                            "raw_b64": base64.b64encode(bytes(raw)).decode('ascii'),
                        }
                        f.write(json.dumps(line, ensure_ascii=False) + "\n")
                        count += 1
                    offset += chunk_size
    return count


async def import_blobs(input_path: str, *, overwrite: bool = True) -> int:
    pool = await get_pool()
    if pool is None:
        raise RuntimeError("AKU_DB_DSN not configured")
    total = 0
    async with pool.acquire() as conn:
        with open(input_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    ds = obj.get('dataset_id')
                    params = obj.get('params') or {}
                    akf = obj.get('ak_function')
                    adp = obj.get('adapter')
                    tz = obj.get('timezone')
                    enc = (obj.get('encoding') or 'raw').lower()
                    raw_b64 = obj.get('raw_b64')
                    if not isinstance(ds, str) or not isinstance(params, dict) or not isinstance(raw_b64, str):
                        continue
                    raw_bytes = base64.b64decode(raw_b64)
                    # direct upsert
                    await conn.execute(
                        """
                        insert into aku_cache_blob (key, dataset_id, params, ak_function, adapter, timezone, raw_data, encoding, updated_at)
                        values ($1,$2,$3,$4,$5,$6,$7,$8, now())
                        on conflict (key) do update set raw_data = excluded.raw_data, encoding = excluded.encoding, ak_function = excluded.ak_function, adapter = excluded.adapter, timezone = excluded.timezone, updated_at = now();
                        """,
                        # compute key the same way as storage
                        __import__('hashlib').sha256(json.dumps({"dataset_id": ds, "params": params}, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest(),
                        ds,
                        json.dumps(params, ensure_ascii=False),
                        akf,
                        adp,
                        tz,
                        raw_bytes,
                        enc,
                    )
                    total += 1
                except Exception:
                    continue
    return total


def main():
    parser = argparse.ArgumentParser(description="AK Unified blob cache export/import")
    sub = parser.add_subparsers(dest='cmd', required=True)

    p_exp = sub.add_parser('export', help='Export blob cache to NDJSON (base64 raw_data)')
    p_exp.add_argument('-o', '--output', required=True, help='Output file')
    p_exp.add_argument('--dataset-prefix', default=None)
    p_exp.add_argument('--updated-after', default=None)
    p_exp.add_argument('--updated-before', default=None)
    p_exp.add_argument('--chunk-size', type=int, default=2000)

    p_imp = sub.add_parser('import', help='Import blob cache from NDJSON (base64 raw_data)')
    p_imp.add_argument('-i', '--input', required=True)

    args = parser.parse_args()
    if args.cmd == 'export':
        cnt = asyncio.run(export_blobs(args.output, dataset_prefix=args.dataset_prefix, updated_after=args.updated_after, updated_before=args.updated_before, chunk_size=args.chunk_size))
        print(f"exported {cnt} blob rows")
    elif args.cmd == 'import':
        cnt = asyncio.run(import_blobs(args.input))
        print(f"imported {cnt} blob rows")


if __name__ == '__main__':
    main()