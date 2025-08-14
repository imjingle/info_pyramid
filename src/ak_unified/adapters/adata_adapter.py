from __future__ import annotations

from typing import Any, Dict, Tuple, Optional, List

import pandas as pd


class ADataAdapterError(RuntimeError):
    pass


def _import_adata():
    try:
        import adata  # type: ignore
        return adata
    except Exception as exc:
        raise ADataAdapterError("Failed to import adata. Install with pip install adata") from exc


def _to_df(obj: Any) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, list):
        return pd.DataFrame(obj)
    if isinstance(obj, dict):
        return pd.DataFrame([obj])
    return pd.DataFrame([])


def call_adata(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    ad = _import_adata()
    # Assume adata has api: get_history(symbol, start, end), get_quotes(symbols)
    if dataset_id.endswith('ohlcv_daily'):
        symbol = params.get('symbol')
        start = params.get('start')
        end = params.get('end')
        try:
            df = ad.get_history(symbol, start, end)
        except Exception as exc:
            raise ADataAdapterError(str(exc)) from exc
        df = _to_df(df)
        if not df.empty:
            df = df.rename(columns={'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume', 'amount': 'amount'})
            if 'symbol' not in df.columns:
                df.insert(0, 'symbol', symbol)
        return ('adata.get_history', df)
    if dataset_id.endswith('quote'):
        symbols: Optional[List[str]] = params.get('symbols')
        try:
            df = ad.get_quotes(symbols) if symbols else ad.get_quotes()
        except Exception as exc:
            raise ADataAdapterError(str(exc)) from exc
        return ('adata.get_quotes', _to_df(df))

    # Industry/Concept lists
    if dataset_id.endswith('board.industry.list.adata'):
        fn = getattr(ad, 'industries', None)
        if fn:
            try:
                return ('adata.industries', _to_df(fn()))
            except Exception as exc:
                raise ADataAdapterError(str(exc)) from exc
        return ('adata.industries', _to_df([]))

    if dataset_id.endswith('board.concept.list.adata'):
        fn = getattr(ad, 'concepts', None)
        if fn:
            try:
                return ('adata.concepts', _to_df(fn()))
            except Exception as exc:
                raise ADataAdapterError(str(exc)) from exc
        return ('adata.concepts', _to_df([]))

    # Constituents
    if dataset_id.endswith('board.industry.cons.adata') or dataset_id.endswith('board.concept.cons.adata'):
        code = params.get('board_code') or params.get('symbol')
        fn = getattr(ad, 'block_stocks', None) or getattr(ad, 'members', None)
        if fn:
            try:
                df = _to_df(fn(code))
            except Exception as exc:
                raise ADataAdapterError(str(exc)) from exc
            if not df.empty:
                df = df.rename(columns={'代码': 'symbol', '名称': 'symbol_name', '权重': 'weight'})
            return ('adata.block_members', df)
        return ('adata.block_members', _to_df([]))

    # Announcements
    if dataset_id.endswith('announcements.adata'):
        symbol = params.get('symbol')
        fn = getattr(ad, 'announcements', None)
        if fn:
            try:
                return ('adata.announcements', _to_df(fn(symbol)))
            except Exception as exc:
                raise ADataAdapterError(str(exc)) from exc
        return ('adata.announcements', _to_df([]))
    return ('adata.unsupported', pd.DataFrame([]))