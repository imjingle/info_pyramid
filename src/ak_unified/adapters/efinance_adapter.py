from __future__ import annotations

from typing import Any, Dict, Tuple, Optional, List

import pandas as pd


class EFinanceAdapterError(RuntimeError):
    pass


def _import_efinance():
    try:
        import efinance as ef  # type: ignore
        return ef
    except Exception as exc:
        raise EFinanceAdapterError("Failed to import efinance. Install with pip install efinance") from exc


def _to_df(obj: Any) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, list):
        return pd.DataFrame(obj)
    if isinstance(obj, dict):
        return pd.DataFrame([obj])
    return pd.DataFrame([])


def call_efinance(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    ef = _import_efinance()
    # Daily OHLCV
    if '.ohlcv_daily' in dataset_id:
        symbol = params.get('symbol')
        start = (params.get('start') or '19900101').replace('-', '')
        end = (params.get('end') or '20990101').replace('-', '')
        # efinance klt: 101=日, 102=周, 103=月; 5/15/30/60 分钟：5/15/30/60
        try:
            df = ef.stock.get_quote_history(symbol, beg=start, end=end, klt=101)
        except Exception as exc:
            raise EFinanceAdapterError(str(exc)) from exc
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.rename(columns={'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume', '成交额': 'amount'})
            if '代码' in df.columns and 'symbol' not in df.columns:
                df.insert(0, 'symbol', df['代码'])
            else:
                df.insert(0, 'symbol', symbol)
        return ('efinance.stock.get_quote_history', _to_df(df))

    # Minute OHLCV
    if '.ohlcv_min' in dataset_id:
        symbol = params.get('symbol')
        start = (params.get('start') or '19900101').replace('-', '')
        end = (params.get('end') or '20990101').replace('-', '')
        freq = str(params.get('freq') or '5').lower().replace('min', '')
        try:
            klt = int(freq)
        except Exception:
            klt = 5
        try:
            df = ef.stock.get_quote_history(symbol, beg=start, end=end, klt=klt)
        except Exception as exc:
            raise EFinanceAdapterError(str(exc)) from exc
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.rename(columns={'日期': 'datetime', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume', '成交额': 'amount'})
            df.insert(0, 'symbol', symbol)
        return ('efinance.stock.get_quote_history', _to_df(df))

    # Realtime quotes
    if '.quote' in dataset_id:
        symbols: Optional[List[str]] = params.get('symbols')
        try:
            if symbols:
                df = ef.stock.get_realtime_quotes(symbols)
            else:
                df = ef.stock.get_realtime_quotes()
        except Exception as exc:
            raise EFinanceAdapterError(str(exc)) from exc
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.rename(columns={'代码': 'symbol', '名称': 'symbol_name', '最新价': 'last', '涨跌幅': 'pct_change', '成交量': 'volume', '成交额': 'amount'})
        return ('efinance.stock.get_realtime_quotes', _to_df(df))

    # Index constituents
    if dataset_id.endswith('index.constituents.efinance'):
        index_code = params.get('index_code') or params.get('symbol')
        df = pd.DataFrame([])
        # try multiple namespaces
        for mod_path in ['ef.stock', 'ef.index']:
            try:
                mod = __import__(mod_path, fromlist=['dummy'])
                fn = getattr(mod, 'get_index_stocks', None)
                if fn:
                    df = _to_df(fn(index_code))
                    break
            except Exception:
                continue
        if not df.empty:
            df = df.rename(columns={'成分券代码': 'symbol', '成分券名称': 'symbol_name', '权重': 'weight'})
            df.insert(0, 'index_symbol', index_code)
        return ('efinance.get_index_stocks', df)

    # Fund flow (individual)
    if dataset_id.endswith('fund_flow.efinance'):
        symbol = params.get('symbol')
        df = pd.DataFrame([])
        for fn_name in ['get_money_flow', 'get_fund_flow']:
            try:
                fn = getattr(ef.stock, fn_name, None)
                if fn:
                    df = _to_df(fn(symbol))
                    break
            except Exception:
                continue
        if not df.empty:
            # Try common columns
            rename = {'日期': 'date', '主力净流入': 'main_inflow', '主力净流出': 'main_outflow', '主力净额': 'net_inflow', '净额': 'net_inflow', '涨跌幅': 'pct_change'}
            df = df.rename(columns=rename)
            if 'symbol' not in df.columns:
                df.insert(0, 'symbol', symbol)
        return ('efinance.stock.fund_flow', df)

    # Industry/concept lists and constituents
    if dataset_id.endswith('board.industry.list.efinance'):
        df = pd.DataFrame([])
        for fn_name in ['get_industries', 'get_industry_plate', 'get_plate_list']:
            try:
                fn = getattr(ef.stock, fn_name, None)
                if fn:
                    df = _to_df(fn())
                    break
            except Exception:
                continue
        return ('efinance.stock.industry_list', df)

    if dataset_id.endswith('board.concept.list.efinance'):
        df = pd.DataFrame([])
        for fn_name in ['get_concepts', 'get_concept_plate']:
            try:
                fn = getattr(ef.stock, fn_name, None)
                if fn:
                    df = _to_df(fn())
                    break
            except Exception:
                continue
        return ('efinance.stock.concept_list', df)

    if dataset_id.endswith('board.industry.cons.efinance') or dataset_id.endswith('board.concept.cons.efinance'):
        plate_code = params.get('board_code') or params.get('symbol')
        df = pd.DataFrame([])
        for fn_name in ['get_plate_stocks', 'get_industry_stocks', 'get_concept_stocks']:
            try:
                fn = getattr(ef.stock, fn_name, None)
                if fn:
                    df = _to_df(fn(plate_code))
                    break
            except Exception:
                continue
        if not df.empty:
            df = df.rename(columns={'代码': 'symbol', '名称': 'symbol_name', '权重': 'weight'})
        return ('efinance.stock.plate_stocks', df)

    # Announcements
    if dataset_id.endswith('announcements.efinance'):
        symbol = params.get('symbol')
        df = pd.DataFrame([])
        for fn_name in ['get_announcement', 'get_announce', 'get_company_announcement']:
            try:
                fn = getattr(ef.stock, fn_name, None)
                if fn:
                    df = _to_df(fn(symbol))
                    break
            except Exception:
                continue
        return ('efinance.stock.announcement', df)

    return ('efinance.unsupported', pd.DataFrame([]))