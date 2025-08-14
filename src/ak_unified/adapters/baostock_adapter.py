from __future__ import annotations

from typing import Any, Dict, Tuple

import pandas as pd


class BaoAdapterError(RuntimeError):
    pass


def _import_baostock():
    try:
        import baostock as bs  # type: ignore
        return bs
    except Exception as exc:
        raise BaoAdapterError("Failed to import baostock. Install with pip install baostock") from exc


def call_baostock(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    bs = _import_baostock()
    lg = bs.login()
    if lg.error_code != '0':
        raise BaoAdapterError(f"baostock login failed: {lg.error_msg}")
    try:
        if dataset_id.endswith('ohlcv_daily'):
            # expects params: symbol, start, end
            symbol = params.get('symbol')
            start = (params.get('start') or '1970-01-01').replace('-', '')
            end = (params.get('end') or '2222-01-01').replace('-', '')
            rs = bs.query_history_k_data_plus(symbol, "date,open,high,low,close,volume,amount", start_date=start, end_date=end, frequency="d", adjustflag="3")
            rows = []
            while rs.error_code == '0' and rs.next():
                rows.append(rs.get_row_data())
            df = pd.DataFrame(rows, columns=rs.fields)
            # map columns
            df = df.rename(columns={"date": "date"})
            for c in ["open","high","low","close","volume","amount"]:
                df[c] = pd.to_numeric(df[c], errors='coerce')
            df.insert(0, 'symbol', symbol)
            return ('baostock.query_history_k_data_plus', df)
        elif dataset_id.endswith('quote'):
            # limited support: return empty placeholder
            return ('baostock.unsupported', pd.DataFrame([]))
        else:
            return ('baostock.unsupported', pd.DataFrame([]))
    finally:
        bs.logout()