from __future__ import annotations

from typing import Any, Dict, Tuple, Optional, List

import pandas as pd


class QStockAdapterError(RuntimeError):
    pass


def _import_qstock():
    try:
        import qstock as qs  # type: ignore
        return qs
    except Exception as exc:
        raise QStockAdapterError("Failed to import qstock. Install with pip install qstock") from exc


def _to_df(obj: Any) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, list):
        return pd.DataFrame(obj)
    if isinstance(obj, dict):
        return pd.DataFrame([obj])
    return pd.DataFrame([])


def call_qstock(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    qs = _import_qstock()
    # Realtime quotes
    if dataset_id.endswith('quote'):
        symbols: Optional[List[str]] = params.get('symbols')
        try:
            df = qs.realtime(symbols) if symbols else qs.realtime()
        except Exception as exc:
            raise QStockAdapterError(str(exc)) from exc
        df = _to_df(df)
        if not df.empty:
            df = df.rename(columns={'代码': 'symbol', '名称': 'symbol_name', '最新': 'last', '涨幅': 'pct_change', '成交': 'amount'})
        return ('qstock.realtime', df)
    # Daily history
    if dataset_id.endswith('ohlcv_daily'):
        symbol = params.get('symbol')
        try:
            df = qs.history(symbol)
        except Exception as exc:
            raise QStockAdapterError(str(exc)) from exc
        df = _to_df(df)
        if not df.empty:
            df = df.rename(columns={'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交额': 'amount'})
            df.insert(0, 'symbol', symbol)
        return ('qstock.history', df)

    # Industry/Concept lists
    if dataset_id.endswith('board.industry.list.qstock'):
        try:
            df = _to_df(qs.industries()) if hasattr(qs, 'industries') else _to_df(qs.block_list('industry'))
        except Exception as exc:
            raise QStockAdapterError(str(exc)) from exc
        return ('qstock.industries', df)

    if dataset_id.endswith('board.concept.list.qstock'):
        try:
            df = _to_df(qs.concepts()) if hasattr(qs, 'concepts') else _to_df(qs.block_list('concept'))
        except Exception as exc:
            raise QStockAdapterError(str(exc)) from exc
        return ('qstock.concepts', df)

    # Constituents
    if dataset_id.endswith('board.industry.cons.qstock') or dataset_id.endswith('board.concept.cons.qstock'):
        code = params.get('board_code') or params.get('symbol')
        try:
            if hasattr(qs, 'block_stocks'):
                df = _to_df(qs.block_stocks(code))
            else:
                df = _to_df(qs.members(code))
        except Exception as exc:
            raise QStockAdapterError(str(exc)) from exc
        if not df.empty:
            df = df.rename(columns={'代码': 'symbol', '名称': 'symbol_name', '权重': 'weight'})
        return ('qstock.block_members', df)

    # Announcements
    if dataset_id.endswith('announcements.qstock'):
        symbol = params.get('symbol')
        try:
            fn = getattr(qs, 'announcements', None)
            df = _to_df(fn(symbol)) if fn else _to_df([])
        except Exception as exc:
            raise QStockAdapterError(str(exc)) from exc
        return ('qstock.announcements', df)
    return ('qstock.unsupported', pd.DataFrame([]))