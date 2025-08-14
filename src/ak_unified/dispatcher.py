from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pandas as _pd

from .schemas.envelope import DataEnvelope, Pagination
from .registry import REGISTRY, DatasetSpec
from .adapters.akshare_adapter import call_akshare
from .storage import get_pool, fetch_records as _db_fetch, upsert_records as _db_upsert, upsert_blob_snapshot as _db_upsert_blob, fetch_blob_snapshot as _db_fetch_blob
import asyncio
from .normalization import apply_and_validate


DEFAULT_TIMEZONE = "Asia/Shanghai"


def _resolve_spec(dataset_id: str) -> DatasetSpec:
    if dataset_id not in REGISTRY:
        raise KeyError(f"Dataset not registered: {dataset_id}")
    return REGISTRY[dataset_id]


def _apply_param_transform(spec: DatasetSpec, params: Dict[str, Any]) -> Dict[str, Any]:
    if spec.param_transform is None:
        return params
    return spec.param_transform(params)


def _postprocess(spec: DatasetSpec, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    if spec.postprocess is None:
        return df
    return spec.postprocess(df, params)


def _envelope(
    spec: DatasetSpec, params: Dict[str, Any], data: List[Dict[str, Any]], currency: Optional[str] = None
) -> DataEnvelope:
    return DataEnvelope(
        category=spec.category,
        domain=spec.domain,
        dataset=spec.dataset_id,
        params=params,
        timezone=DEFAULT_TIMEZONE,
        currency=currency,
        data=data,
        pagination=Pagination(offset=0, limit=len(data), total=len(data)),
    )


def fetch_data(dataset_id: str, params: Optional[Dict[str, Any]] = None, *, ak_function: Optional[str] = None, allow_fallback: bool = False, use_cache: bool = True, use_blob: bool = True, store_blob: bool = True) -> DataEnvelope:
    spec = _resolve_spec(dataset_id)
    params = params or {}

    # computed dataset
    if spec.compute is not None:
        df = spec.compute(params)
        records = df.to_dict(orient="records")
        env = _envelope(spec, params, records)
        env.ak_function = "computed"
        env.data_source = spec.source or "computed"
        return env

    # try cache first (only for non-realtime datasets)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    pool = loop.run_until_complete(get_pool()) if (use_cache or use_blob or store_blob) else None
    cached: List[Dict[str, Any]] = []
    is_realtime = 'quote' in dataset_id
    time_field = "datetime" if ("ohlcv_min" in dataset_id or dataset_id.endswith(".ohlcva_min")) else "date"
    symbol = params.get("symbol")
    index_symbol = params.get("index_code") or params.get("index_symbol")
    board_code = params.get("board_code") or (params.get("symbol") if "board" in dataset_id else None)
    start = params.get("start")
    end = params.get("end")
    # exact request-level blob cache (before upstream) if enabled and not realtime
    if pool is not None and use_blob and not is_realtime:
        try:
            res = loop.run_until_complete(_db_fetch_blob(pool, dataset_id, params))
            if res is not None:
                raw_records, meta = res
                records = apply_and_validate(dataset_id, raw_records)
                env = _envelope(spec, params, records)
                env.ak_function = meta.get("ak_function") or 'cache-blob'
                env.data_source = meta.get("adapter") or 'postgresql-blob'
                return env
        except Exception:
            pass
    if pool is not None and not is_realtime:
        cached = loop.run_until_complete(_db_fetch(pool, dataset_id, symbol=symbol, index_symbol=index_symbol, board_code=board_code, start=start, end=end, time_field=time_field))
        # If fully covered by cache for date-based datasets, return immediately from cache
        if time_field == 'date' and start and end and cached:
            try:
                cdf = _pd.DataFrame(cached)
                if 'date' in cdf.columns:
                    want = _pd.date_range(start=start, end=end, freq='D').strftime('%Y-%m-%d')
                    have = _pd.to_datetime(cdf['date']).dt.strftime('%Y-%m-%d')
                    missing = sorted(list(set(want) - set(have)))
                    if not missing:
                        env = _envelope(spec, params, cached)
                        env.ak_function = 'cache'
                        env.data_source = 'postgresql'
                        return env
            except Exception:
                pass

    ak_params = _apply_param_transform(spec, params)

    # dispatch adapter (with gap fill for date-based datasets)
    new_records: List[Dict[str, Any]] = []
    if pool is not None and not is_realtime and time_field == 'date' and start and end and cached:
        # compute missing date spans
        try:
            want = _pd.date_range(start=start, end=end, freq='D').strftime('%Y-%m-%d').tolist()
            have = _pd.to_datetime(_pd.DataFrame(cached)['date']).dt.strftime('%Y-%m-%d').tolist()
            missing = [d for d in want if d not in set(have)]
            def spans(ds: List[str]) -> List[Tuple[str, str]]:
                if not ds:
                    return []
                out: List[Tuple[str, str]] = []
                s = e = ds[0]
                for d in ds[1:]:
                    if ( _pd.to_datetime(d) - _pd.to_datetime(e) ).days == 1:
                        e = d
                    else:
                        out.append((s, e))
                        s = e = d
                out.append((s, e))
                return out
            miss_spans = spans(missing)
        except Exception:
            miss_spans = [(start, end)]
        # fetch each span
        for s1, e1 in miss_spans:
            part_params = dict(params)
            part_params['start'] = s1
            part_params['end'] = e1
            part_ak_params = _apply_param_transform(spec, part_params)
            if spec.adapter == "akshare":
                fn_used, df = call_akshare(spec.ak_functions, part_ak_params, field_mapping=spec.field_mapping, allow_fallback=allow_fallback, function_name=ak_function)
            elif spec.adapter == "baostock":
                from .adapters.baostock_adapter import call_baostock
                fn_used, df = call_baostock(dataset_id, part_ak_params)
            elif spec.adapter == "mootdx":
                from .adapters.mootdx_adapter import call_mootdx
                fn_used, df = call_mootdx(dataset_id, part_ak_params)
            elif spec.adapter == "qmt":
                from .adapters.qmt_adapter import call_qmt
                fn_used, df = call_qmt(dataset_id, part_ak_params)
            elif spec.adapter == "efinance":
                from .adapters.efinance_adapter import call_efinance
                fn_used, df = call_efinance(dataset_id, part_ak_params)
            elif spec.adapter == "qstock":
                from .adapters.qstock_adapter import call_qstock
                fn_used, df = call_qstock(dataset_id, part_ak_params)
            elif spec.adapter == "adata":
                from .adapters.adata_adapter import call_adata
                fn_used, df = call_adata(dataset_id, part_ak_params)
            elif spec.adapter == "yfinance":
                from .adapters.yfinance_adapter import call_yfinance
                fn_used, df = call_yfinance(dataset_id, part_ak_params)
            elif spec.adapter == "alphavantage":
                from .adapters.alphavantage_adapter import call_alphavantage
                fn_used, df = call_alphavantage(dataset_id, part_ak_params)
            else:
                raise RuntimeError(f"Unknown adapter: {spec.adapter}")
            df = _postprocess(spec, df, part_params)
            part_records = df.to_dict(orient="records")
            new_records.extend(part_records)
        records = cached + new_records
        # upsert
        if new_records:
            try:
                loop.run_until_complete(_db_upsert(pool, dataset_id, new_records))
            except Exception:
                pass
        env = _envelope(spec, params, records)
        env.ak_function = 'cache+source'
        env.data_source = spec.source
        return env

    # minute-level gap fill for datetime-based datasets
    new_records = []
    if pool is not None and not is_realtime and time_field == 'datetime' and start and end and cached:
        try:
            freq = str(params.get('freq') or 'min5').lower()
            # normalize minutes step
            if freq.startswith('min'):
                step_min = int(freq.replace('min','') or '5')
            else:
                step_min = int(freq)
        except Exception:
            step_min = 5
        try:
            cdf = _pd.DataFrame(cached)
            cdf = cdf.sort_values('datetime')
            dt_cached = _pd.to_datetime(cdf['datetime'])
            start_dt = _pd.to_datetime(start)
            end_dt = _pd.to_datetime(end)
            gaps: List[Tuple[str, str]] = []
            # before first
            if len(dt_cached) == 0 or dt_cached.iloc[0] > start_dt:
                gaps.append((start_dt.isoformat(), (dt_cached.iloc[0] - _pd.Timedelta(minutes=step_min)).isoformat() if len(dt_cached) else end_dt.isoformat()))
            # internal gaps
            for i in range(1, len(dt_cached)):
                prev = dt_cached.iloc[i-1]
                cur = dt_cached.iloc[i]
                if (cur - prev) > _pd.Timedelta(minutes=step_min):
                    s1 = prev + _pd.Timedelta(minutes=step_min)
                    e1 = cur - _pd.Timedelta(minutes=step_min)
                    if s1 <= e1:
                        gaps.append((s1.isoformat(), e1.isoformat()))
            # after last
            if len(dt_cached) and dt_cached.iloc[-1] < end_dt:
                s2 = dt_cached.iloc[-1] + _pd.Timedelta(minutes=step_min)
                gaps.append((s2.isoformat(), end_dt.isoformat()))
        except Exception:
            gaps = [(start, end)]
        # fetch each gap
        for s1, e1 in gaps:
            part_params = dict(params)
            part_params['start'] = s1
            part_params['end'] = e1
            part_ak_params = _apply_param_transform(spec, part_params)
            if spec.adapter == "akshare":
                fn_used, df = call_akshare(spec.ak_functions, part_ak_params, field_mapping=spec.field_mapping, allow_fallback=allow_fallback, function_name=ak_function)
            elif spec.adapter == "baostock":
                from .adapters.baostock_adapter import call_baostock
                fn_used, df = call_baostock(dataset_id, part_ak_params)
            elif spec.adapter == "mootdx":
                from .adapters.mootdx_adapter import call_mootdx
                fn_used, df = call_mootdx(dataset_id, part_ak_params)
            elif spec.adapter == "qmt":
                from .adapters.qmt_adapter import call_qmt
                fn_used, df = call_qmt(dataset_id, part_ak_params)
            elif spec.adapter == "efinance":
                from .adapters.efinance_adapter import call_efinance
                fn_used, df = call_efinance(dataset_id, part_ak_params)
            elif spec.adapter == "qstock":
                from .adapters.qstock_adapter import call_qstock
                fn_used, df = call_qstock(dataset_id, part_ak_params)
            elif spec.adapter == "adata":
                from .adapters.adata_adapter import call_adata
                fn_used, df = call_adata(dataset_id, part_ak_params)
            elif spec.adapter == "yfinance":
                from .adapters.yfinance_adapter import call_yfinance
                fn_used, df = call_yfinance(dataset_id, part_ak_params)
            elif spec.adapter == "alphavantage":
                from .adapters.alphavantage_adapter import call_alphavantage
                fn_used, df = call_alphavantage(dataset_id, part_ak_params)
            else:
                raise RuntimeError(f"Unknown adapter: {spec.adapter}")
            df = _postprocess(spec, df, part_params)
            part_records = df.to_dict(orient="records")
            new_records.extend(part_records)
        records = cached + new_records
        if new_records:
            try:
                loop.run_until_complete(_db_upsert(pool, dataset_id, new_records))
            except Exception:
                pass
        env = _envelope(spec, params, records)
        env.ak_function = 'cache+source'
        env.data_source = spec.source
        return env

    # no cache or not eligible for gap fill: single fetch
    if spec.adapter == "akshare":
        fn_used, df = call_akshare(
            spec.ak_functions,
            ak_params,
            field_mapping=spec.field_mapping,
            allow_fallback=allow_fallback,
            function_name=ak_function,
        )
    elif spec.adapter == "baostock":
        from .adapters.baostock_adapter import call_baostock
        fn_used, df = call_baostock(dataset_id, ak_params)
    elif spec.adapter == "mootdx":
        from .adapters.mootdx_adapter import call_mootdx
        fn_used, df = call_mootdx(dataset_id, ak_params)
    elif spec.adapter == "qmt":
        from .adapters.qmt_adapter import call_qmt
        fn_used, df = call_qmt(dataset_id, ak_params)
    elif spec.adapter == "efinance":
        from .adapters.efinance_adapter import call_efinance
        fn_used, df = call_efinance(dataset_id, ak_params)
    elif spec.adapter == "qstock":
        from .adapters.qstock_adapter import call_qstock
        fn_used, df = call_qstock(dataset_id, ak_params)
    elif spec.adapter == "adata":
        from .adapters.adata_adapter import call_adata
        fn_used, df = call_adata(dataset_id, ak_params)
    elif spec.adapter == "yfinance":
        from .adapters.yfinance_adapter import call_yfinance
        fn_used, df = call_yfinance(dataset_id, ak_params)
    elif spec.adapter == "alphavantage":
        from .adapters.alphavantage_adapter import call_alphavantage
        fn_used, df = call_alphavantage(dataset_id, ak_params)
    else:
        raise RuntimeError(f"Unknown adapter: {spec.adapter}")

    df = _postprocess(spec, df, params)
    raw_records = df.to_dict(orient="records")
    records = apply_and_validate(dataset_id, raw_records)

    # merge with cache and upsert
    if pool is not None and records and not is_realtime:
        try:
            # naive merge by row_key uniqueness is handled in upsert
            loop.run_until_complete(_db_upsert(pool, dataset_id, records))
            # store request-level blob for exact replay (pickle)
            if store_blob:
                try:
                    loop.run_until_complete(_db_upsert_blob(pool, dataset_id, params, ak_function=fn_used, adapter=spec.adapter, timezone=DEFAULT_TIMEZONE, raw_obj=raw_records))
                except Exception:
                    pass
            if cached:
                # return union (cached + fresh unique)
                # simple de-dup by (symbol/index_symbol/board_code,time)
                def key(r: Dict[str, Any]):
                    return (r.get('symbol') or r.get('index_symbol') or r.get('board_code'), r.get('date') or r.get('datetime'))
                merged = {key(r): r for r in cached}
                for r in records:
                    merged[key(r)] = r
                records = list(merged.values())
        except Exception:
            pass

    env = _envelope(spec, params, records)
    env.ak_function = fn_used
    env.data_source = spec.source
    return env


# ------------- Convenience APIs -------------

def get_ohlcv(symbol: str, start: Optional[str] = None, end: Optional[str] = None, adjust: str = "none", *, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    params = {"symbol": symbol, "start": start, "end": end, "adjust": adjust}
    return fetch_data("securities.equity.cn.ohlcv_daily", params, ak_function=ak_function, allow_fallback=allow_fallback)


def get_market_quote(*, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    return fetch_data("securities.equity.cn.quote", {}, ak_function=ak_function, allow_fallback=allow_fallback)


def get_index_constituents(index_code: str, *, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    return fetch_data("market.index.constituents", {"index_code": index_code}, ak_function=ak_function, allow_fallback=allow_fallback)


def get_macro_indicator(region: str, indicator_id: str, **kwargs: Any) -> DataEnvelope:
    key = (region.upper(), indicator_id.lower())
    mapping = {
        ("CN", "ppi"): "macro.cn.ppi",
        ("CN", "pmi"): "macro.cn.pmi",
        ("CN", "gdp"): "macro.cn.gdp",
    }
    dataset = mapping.get(key)
    if not dataset:
        raise KeyError(f"Macro indicator not mapped: region={region} indicator_id={indicator_id}")
    ak_function = kwargs.pop("ak_function", None)
    allow_fallback = kwargs.pop("allow_fallback", False)
    return fetch_data(dataset, kwargs, ak_function=ak_function, allow_fallback=allow_fallback)


def get_fund_nav(fund_code: str, start: Optional[str] = None, end: Optional[str] = None, *, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    is_etf_like = fund_code.isdigit() and fund_code.startswith(("5", "1"))
    dataset = "securities.fund.cn.nav" if is_etf_like else "securities.fund.cn.nav_open"
    params: Dict[str, Any] = {"fund_code": fund_code}
    if dataset == "securities.fund.cn.nav":
        params.update({"start": start, "end": end})
    return fetch_data(dataset, params, ak_function=ak_function, allow_fallback=allow_fallback)


def get_ohlcva(symbol: str, start: Optional[str] = None, end: Optional[str] = None, adjust: str = "none", *, ak_function: Optional[str] = None, allow_fallback: bool = False) -> DataEnvelope:
    params = {"symbol": symbol, "start": start, "end": end, "adjust": adjust}
    return fetch_data("securities.equity.cn.ohlcva_daily", params, ak_function=ak_function, allow_fallback=allow_fallback)