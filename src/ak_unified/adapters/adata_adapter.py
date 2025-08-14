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
    return ('adata.unsupported', pd.DataFrame([]))