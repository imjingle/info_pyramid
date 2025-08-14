from __future__ import annotations

import threading
from typing import Any, Dict, Tuple

import pandas as pd


class BaoAdapterError(RuntimeError):
    pass


_lock = threading.Lock()


def _import_baostock():
    try:
        import baostock as bs  # type: ignore
        return bs
    except Exception as exc:
        raise BaoAdapterError("Failed to import baostock. Install with pip install baostock") from exc


def _with_session(run):
    bs = _import_baostock()
    with _lock:
        lg = bs.login()
        if lg.error_code != '0':
            raise BaoAdapterError(f"baostock login failed: {lg.error_msg}")
        try:
            return run(bs)
        finally:
            bs.logout()


def _iter_resultset(rs):
    rows = []
    while rs.error_code == '0' and rs.next():
        rows.append(rs.get_row_data())
    return rows, rs.fields


def call_baostock(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    def handle(bs):
        if dataset_id.endswith('ohlcv_daily'):
            symbol = params.get('symbol')
            start = (params.get('start') or '19700101').replace('-', '')
            end = (params.get('end') or '22220101').replace('-', '')
            rs = bs.query_history_k_data_plus(symbol, "date,open,high,low,close,volume,amount", start_date=start, end_date=end, frequency="d", adjustflag="3")
            rows, fields = _iter_resultset(rs)
            df = pd.DataFrame(rows, columns=fields)
            for c in ["open","high","low","close","volume","amount"]:
                df[c] = pd.to_numeric(df[c], errors='coerce')
            df.insert(0, 'symbol', symbol)
            return 'baostock.query_history_k_data_plus', df

        if dataset_id == 'market.calendar.baostock':
            start = (params.get('start') or '1990-12-19')
            end = (params.get('end') or '2099-12-31')
            rs = bs.query_trade_dates(start_date=start, end_date=end)
            rows, fields = _iter_resultset(rs)
            df = pd.DataFrame(rows, columns=fields)
            df = df.rename(columns={"calendar_date": "date", "is_trading_day": "is_trading_day"})
            df["is_trading_day"] = df["is_trading_day"].astype(str) == '1'
            return 'baostock.query_trade_dates', df

        if dataset_id == 'securities.industry.cn.class.baostock':
            rs = bs.query_stock_industry()
            rows, fields = _iter_resultset(rs)
            df = pd.DataFrame(rows, columns=fields)
            df = df.rename(columns={"code": "symbol", "industry": "industry", "updateDate": "update_date"})
            return 'baostock.query_stock_industry', df

        if dataset_id == 'market.index.constituents.baostock':
            index_code = params.get('index_code') or params.get('symbol')
            if index_code in ('000300.SH', 'sh000300'):
                rs = bs.query_hs300_stocks()
                tag = 'hs300'
            elif index_code in ('000016.SH', 'sh000016'):
                rs = getattr(bs, 'query_sz50_stocks', None)() if hasattr(bs, 'query_sz50_stocks') else None
                if rs is None:
                    return 'baostock.unsupported', pd.DataFrame([])
                tag = 'sz50'
            elif index_code in ('000905.SH', 'sh000905'):
                rs = bs.query_zz500_stocks()
                tag = 'zz500'
            else:
                return 'baostock.unsupported', pd.DataFrame([])
            rows, fields = _iter_resultset(rs)
            df = pd.DataFrame(rows, columns=fields)
            df = df.rename(columns={"code": "symbol", "updateDate": "date"})
            df.insert(0, 'index_symbol', index_code)
            return f'baostock.query_{tag}_stocks', df

        if dataset_id == 'securities.equity.cn.adjust_factor.baostock':
            symbol = params.get('symbol')
            start = (params.get('start') or '19700101').replace('-', '')
            end = (params.get('end') or '22220101').replace('-', '')
            if hasattr(bs, 'query_adjust_factor'):
                rs = bs.query_adjust_factor(code=symbol, start_date=start, end_date=end)
                rows, fields = _iter_resultset(rs)
                df = pd.DataFrame(rows, columns=fields)
                df = df.rename(columns={"code": "symbol", "adjustfactor": "adjust_factor", "tradeDate": "date"})
                df["adjust_factor"] = pd.to_numeric(df["adjust_factor"], errors='coerce')
                return 'baostock.query_adjust_factor', df
            # fallback: not supported
            return 'baostock.unsupported', pd.DataFrame([])

        return 'baostock.unsupported', pd.DataFrame([])

    tag, df = _with_session(handle)
    return tag, df