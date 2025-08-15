from __future__ import annotations

from typing import Any, Dict, Tuple

import pandas as pd


class MooAdapterError(RuntimeError):
    pass


def _import_mootdx_quotes():
    try:
        from mootdx.quotes import Quotes  # type: ignore
        return Quotes.factory('std')
    except Exception as exc:
        raise MooAdapterError("Failed to import mootdx. Install with pip install mootdx") from exc


def _import_mootdx_reader():
    try:
        from mootdx.reader import Reader  # type: ignore
        return Reader.factory(market='std')
    except Exception:
        return None


def call_mootdx(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    # Daily OHLCV
    if dataset_id.endswith('ohlcv_daily'):
        q = _import_mootdx_quotes()
        symbol = params.get('symbol')
        if symbol and symbol.endswith('.SH'):
            market = 1
            code = symbol[:6]
        elif symbol and symbol.endswith('.SZ'):
            market = 0
            code = symbol[:6]
        else:
            market = 1
            code = (symbol or '')[:6]
        try:
            df = q.bars(symbol=code, frequency=9, start=0, offset=2000, market=market)
        except Exception as exc:
            raise MooAdapterError(str(exc)) from exc
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.rename(columns={"open": "open", "high": "high", "low": "low", "close": "close", "vol": "volume"})
            df.insert(0, 'symbol', symbol)
            return ('mootdx.bars', df)
        return ('mootdx.bars', pd.DataFrame([]))

    # Minute OHLCV (5/15/30/60)
    if dataset_id.endswith('ohlcv_min'):
        q = _import_mootdx_quotes()
        symbol = params.get('symbol')
        if symbol and symbol.endswith('.SH'):
            market = 1
            code = symbol[:6]
        elif symbol and symbol.endswith('.SZ'):
            market = 0
            code = symbol[:6]
        else:
            market = 1
            code = (symbol or '')[:6]
        freq = str(params.get('freq') or '').lower()
        # map to TDX frequency code
        freq_map = {'min5': 0, '5': 0, 'min15': 1, '15': 1, 'min30': 2, '30': 2, 'min60': 3, '60': 3}
        fcode = freq_map.get(freq, 0)
        try:
            df = q.bars(symbol=code, frequency=fcode, start=0, offset=2000, market=market)
        except Exception as exc:
            raise MooAdapterError(str(exc)) from exc
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.rename(columns={"open": "open", "high": "high", "low": "low", "close": "close", "vol": "volume"})
            df.insert(0, 'symbol', symbol)
            return (f'mootdx.bars_{fcode}', df)
        return (f'mootdx.bars_{fcode}', pd.DataFrame([]))

    # Blocks (industry/concept)
    if dataset_id in ('securities.board.cn.industry.blocks.mootdx', 'securities.board.cn.concept.blocks.mootdx'):
        reader = _import_mootdx_reader()
        if reader is None:
            return ('mootdx.reader_unavailable', pd.DataFrame([]))
        try:
            cat = 'hy' if 'industry' in dataset_id else 'gn'
            df = reader.block(symbol=cat)
            # Expected columns: code, name, blockname
            if isinstance(df, pd.DataFrame) and not df.empty:
                cols = {c: c for c in df.columns}
                cols.update({"code": "symbol", "blockname": "board_name"})
                df = df.rename(columns=cols)
                return (f'mootdx.block_{cat}', df)
        except Exception:
            return ('mootdx.block_error', pd.DataFrame([]))
        return ('mootdx.block_empty', pd.DataFrame([]))

    # Index constituents via blocks (zs)
    if dataset_id == 'market.index.constituents.mootdx':
        reader = _import_mootdx_reader()
        if reader is None:
            return ('mootdx.reader_unavailable', pd.DataFrame([]))
        try:
            df = reader.block(symbol='zs')
            # Filter by index code/name if provided
            idx = params.get('index_code') or params.get('symbol')
            if isinstance(df, pd.DataFrame) and not df.empty and idx:
                f = df[df['blockname'].astype(str).str.contains(str(idx)) | df['code'].astype(str).str.contains(str(idx))]
                if not f.empty:
                    f = f.rename(columns={"code": "symbol"})
                    f.insert(0, 'index_symbol', idx)
                    return ('mootdx.block_zs', f)
            return ('mootdx.block_zs', pd.DataFrame([]))
        except Exception:
            return ('mootdx.block_error', pd.DataFrame([]))

    # Adjust factor (xdxr)
    if dataset_id == 'securities.equity.cn.adjust_factor.mootdx':
        q = _import_mootdx_quotes()
        symbol = params.get('symbol')
        code = (symbol or '')[:6]
        try:
            df = q.xdxr(symbol=code)
            if isinstance(df, pd.DataFrame):
                df = df.rename(columns={"category": "category", "date": "date"})
                df.insert(0, 'symbol', symbol)
                return ('mootdx.xdxr', df)
        except Exception:
            return ('mootdx.xdxr_error', pd.DataFrame([]))
        return ('mootdx.xdxr', pd.DataFrame([]))

    # Fundamentals via reader finance (if available)
    if dataset_id == 'securities.equity.cn.fundamentals.mootdx':
        reader = _import_mootdx_reader()
        if reader is None:
            return ('mootdx.reader_unavailable', pd.DataFrame([]))
        symbol = params.get('symbol')
        code = (symbol or '')[:6]
        try:
            if hasattr(reader, 'finance'):
                df = reader.finance(symbol=code)
            else:
                df = pd.DataFrame([])
            if isinstance(df, pd.DataFrame):
                df.insert(0, 'symbol', symbol)
                return ('mootdx.finance', df)
        except Exception:
            return ('mootdx.finance_error', pd.DataFrame([]))
        return ('mootdx.finance', pd.DataFrame([]))

    return ('mootdx.unsupported', pd.DataFrame([]))