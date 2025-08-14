from __future__ import annotations

import os
import time
from typing import Any, Dict, Tuple, Optional, List
import pandas as pd


class IBKRAdapterError(RuntimeError):
    pass


def _import_ib() -> Any:
    try:
        from ib_insync import IB, Stock  # type: ignore
        return IB, Stock
    except Exception as exc:
        raise IBKRAdapterError("Failed to import ib_insync. Install with pip install ib-insync or uv sync --extra ibkr") from exc


def _ib_connect() -> Any:
    IB, _ = _import_ib()
    host = os.getenv('AKU_IB_HOST', '127.0.0.1')
    port = int(os.getenv('AKU_IB_PORT', '7497'))
    client_id = int(os.getenv('AKU_IB_CLIENT_ID', '1'))
    ib = IB()
    if not ib.connect(host, port, clientId=client_id, timeout=10):
        raise IBKRAdapterError(f"Could not connect to IBKR at {host}:{port}")
    return ib


def _make_stock(symbol: str, exchange: Optional[str], currency: Optional[str]) -> Any:
    _, Stock = _import_ib()
    ex = exchange or 'SMART'
    cur = currency or ('HKD' if (exchange or '').upper() in {'SEHK', 'HKEX'} else 'USD')
    return Stock(symbol=symbol, exchange=ex, currency=cur)


def _qualify(ib: Any, contract: Any) -> Any:
    q = ib.qualifyContracts(contract)
    if not q:
        raise IBKRAdapterError("Failed to qualify contract")
    return q[0]


def _bars_to_df(bars: List[Any], is_intraday: bool) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame([])
    rows = []
    for b in bars:
        rows.append({
            ('datetime' if is_intraday else 'date'): b.date.strftime('%Y-%m-%d %H:%M:%S') if is_intraday else b.date.strftime('%Y-%m-%d'),
            'open': float(b.open),
            'high': float(b.high),
            'low': float(b.low),
            'close': float(b.close),
            'volume': float(b.volume or 0),
        })
    return pd.DataFrame(rows)


def _duration_from_range(start: Optional[str], end: Optional[str], fallback_days: int) -> str:
    # IB uses durationStr like '180 D' or '1 Y'
    if not start or not end:
        return f"{fallback_days} D"
    try:
        from datetime import datetime
        s = datetime.fromisoformat(str(start))
        e = datetime.fromisoformat(str(end))
        days = max(1, (e - s).days)
        if days >= 365:
            years = max(1, days // 365)
            return f"{years} Y"
        return f"{days} D"
    except Exception:
        return f"{fallback_days} D"


def _bar_size_from_freq(freq: Optional[str]) -> str:
    f = str(freq or '1d').lower()
    if f in {'1d','d','day','daily'}:
        return '1 day'
    if 'min' in f:
        try:
            m = int(f.replace('min',''))
        except Exception:
            m = 5
        if m <= 1:
            return '1 min'
        if m <= 5:
            return '5 mins'
        if m <= 15:
            return '15 mins'
        if m <= 30:
            return '30 mins'
        return '1 hour'
    try:
        n = int(f)
        if n >= 60:
            return '1 hour'
        return f'{n} mins'
    except Exception:
        return '5 mins'


def _hist_ohlcv(ib: Any, symbol: str, exchange: Optional[str], currency: Optional[str], start: Optional[str], end: Optional[str], freq: Optional[str]) -> pd.DataFrame:
    contract = _make_stock(symbol, exchange, currency)
    contract = _qualify(ib, contract)
    bar_size = _bar_size_from_freq(freq)
    is_intraday = 'min' in bar_size or 'hour' in bar_size
    # IB intraday history is limited; use a reasonable duration fallback
    duration = _duration_from_range(start, end, fallback_days=(365 if not is_intraday else 30))
    what = 'TRADES'
    bars = ib.reqHistoricalData(contract, endDateTime='', durationStr=duration, barSizeSetting=bar_size, whatToShow=what, useRTH=False, formatDate=1)
    df = _bars_to_df(bars, is_intraday=is_intraday)
    if not df.empty:
        df.insert(0, 'symbol', symbol)
    return df


def _quote(ib: Any, symbol: str, exchange: Optional[str], currency: Optional[str]) -> pd.DataFrame:
    contract = _make_stock(symbol, exchange, currency)
    contract = _qualify(ib, contract)
    ticker = ib.reqMktData(contract, '', False, False)
    # wait briefly for snapshot
    ib.sleep(1.0)
    last = ticker.last if hasattr(ticker, 'last') else None
    close = ticker.close if hasattr(ticker, 'close') else None
    bid = ticker.bid if hasattr(ticker, 'bid') else None
    ask = ticker.ask if hasattr(ticker, 'ask') else None
    volume = ticker.volume if hasattr(ticker, 'volume') else None
    change = None
    pct = None
    try:
        if last is not None and close is not None:
            change = float(last) - float(close)
            pct = change / float(close) * 100.0 if close else None
    except Exception:
        pass
    return pd.DataFrame([{
        'symbol': symbol,
        'last': float(last) if last is not None else None,
        'prev_close': float(close) if close is not None else None,
        'change': change,
        'pct_change': pct,
        'bid': float(bid) if bid is not None else None,
        'ask': float(ask) if ask is not None else None,
        'volume': float(volume) if volume is not None else None,
    }])


def _fundamental(ib: Any, symbol: str, exchange: Optional[str], currency: Optional[str], report_type: str) -> pd.DataFrame:
    contract = _make_stock(symbol, exchange, currency)
    contract = _qualify(ib, contract)
    xml: str = ib.reqFundamentalData(contract, report_type)
    if not xml:
        return pd.DataFrame([])
    # minimal XML to flat dict
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml)
        flat: Dict[str, Any] = {}
        def walk(node, prefix=''):
            tag = (node.tag or '').split('}')[-1]
            key = f"{prefix}.{tag}" if prefix else tag
            if list(node):
                for c in node:
                    walk(c, key)
            else:
                text = (node.text or '').strip()
                if text:
                    if key in flat:
                        # handle duplicates by suffixing index
                        i = 2
                        while f"{key}_{i}" in flat:
                            i += 1
                        flat[f"{key}_{i}"] = text
                    else:
                        flat[key] = text
        walk(root)
        return pd.DataFrame([{'symbol': symbol, 'report_type': report_type, **flat}])
    except Exception:
        # store raw xml in a single column for auditing
        return pd.DataFrame([{'symbol': symbol, 'report_type': report_type, 'raw_xml': xml}])


def call_ibkr(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    ib = _ib_connect()
    try:
        symbol = (params.get('symbol') or '').upper()
        exchange = params.get('exchange')
        currency = params.get('currency')
        if dataset_id.endswith('.ohlcv_daily.ibkr'):
            df = _hist_ohlcv(ib, symbol, exchange, currency, params.get('start'), params.get('end'), '1d')
            return ('ibkr.reqHistoricalData_1d', df)
        if dataset_id.endswith('.ohlcv_min.ibkr'):
            df = _hist_ohlcv(ib, symbol, exchange, currency, params.get('start'), params.get('end'), params.get('freq'))
            return ('ibkr.reqHistoricalData_min', df)
        if dataset_id.endswith('.quote.ibkr'):
            df = _quote(ib, symbol, exchange, currency)
            return ('ibkr.reqMktData', df)
        if dataset_id.endswith('.fundamentals.overview.ibkr'):
            df = _fundamental(ib, symbol, exchange, currency, 'CompanyOverview')
            return ('ibkr.reqFundamentalData_CompanyOverview', df)
        if dataset_id.endswith('.fundamentals.statements.ibkr'):
            df = _fundamental(ib, symbol, exchange, currency, 'ReportsFinStatements')
            return ('ibkr.reqFundamentalData_ReportsFinStatements', df)
        if dataset_id.endswith('.fundamentals.ratios.ibkr'):
            df = _fundamental(ib, symbol, exchange, currency, 'Ratios')
            return ('ibkr.reqFundamentalData_Ratios', df)
        if dataset_id.endswith('.fundamentals.snapshot.ibkr'):
            df = _fundamental(ib, symbol, exchange, currency, 'ReportSnapshot')
            return ('ibkr.reqFundamentalData_ReportSnapshot', df)
        return ('ibkr.unsupported', pd.DataFrame([]))
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass