from __future__ import annotations

from typing import Any, Dict, Tuple

import pandas as pd


class MooAdapterError(RuntimeError):
    pass


def _import_mootdx():
    try:
        from mootdx.quotes import Quotes  # type: ignore
        return Quotes.factory('std')
    except Exception as exc:
        raise MooAdapterError("Failed to import mootdx. Install with pip install mootdx") from exc


def call_mootdx(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    q = _import_mootdx()
    if dataset_id.endswith('ohlcv_daily'):
        symbol = params.get('symbol')
        # mootdx expects code + market; heuristic: .SH/.SZ suffix
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
            df = q.bars(symbol=code, frequency=9, start=0, offset=1200, market=market)
        except Exception as exc:
            raise MooAdapterError(str(exc)) from exc
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.rename(columns={"open": "open", "high": "high", "low": "low", "close": "close", "vol": "volume"})
            df.insert(0, 'symbol', symbol)
            return ('mootdx.bars', df)
        return ('mootdx.bars', pd.DataFrame([]))
    return ('mootdx.unsupported', pd.DataFrame([]))