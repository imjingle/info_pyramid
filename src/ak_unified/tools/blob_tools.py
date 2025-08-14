from __future__ import annotations

import argparse
import asyncio
import base64
import json
from typing import Any, Dict, Optional

from ..storage import get_pool


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