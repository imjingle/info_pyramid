from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any, Dict, Optional

from ..storage import get_pool, cache_stats, purge_records


async def export_cache(output: str, dataset_prefix: Optional[str] = None) -> int:
    pool = await get_pool()
    if pool is None:
        raise RuntimeError("AKU_DB_DSN not configured")
    # stream all rows optionally filtered by dataset prefix
    async with pool.acquire() as conn:
        if dataset_prefix:
            sql = "select dataset_id, record from aku_cache where dataset_id like $1 order by dataset_id"
            stmt = await conn.prepare(sql)
            rows = await stmt.fetch(dataset_prefix + '%')
        else:
            rows = await conn.fetch("select dataset_id, record from aku_cache order by dataset_id")
    count = 0
    with open(output, 'w', encoding='utf-8') as f:
        for ds, rec in rows:
            obj = rec if isinstance(rec, dict) else json.loads(rec)
            f.write(json.dumps({"dataset_id": ds, "record": obj}, ensure_ascii=False) + "\n")
            count += 1
    return count


async def import_cache(input_path: str, dataset_prefix: Optional[str] = None) -> int:
    pool = await get_pool()
    if pool is None:
        raise RuntimeError("AKU_DB_DSN not configured")
    # read NDJSON and upsert per batch
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
                batch.setdefault(ds, []).append(rec)
                total += 1
                if sum(len(v) for v in batch.values()) >= 1000:
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
    p_imp = sub.add_parser('import', help='Import cache from NDJSON')
    p_imp.add_argument('-i', '--input', required=True, help='Input NDJSON file')
    p_imp.add_argument('--dataset-prefix', default=None, help='Filter dataset_id by prefix while importing')

    args = parser.parse_args()
    if args.cmd == 'export':
        cnt = asyncio.run(export_cache(args.output, args.dataset_prefix))
        print(f"exported {cnt} rows")
    elif args.cmd == 'import':
        cnt = asyncio.run(import_cache(args.input, args.dataset_prefix))
        print(f"imported {cnt} rows")


if __name__ == '__main__':
    main()