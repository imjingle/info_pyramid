from __future__ import annotations

import os
from typing import Any, Dict, Tuple
from urllib import request, parse
import json
import pandas as pd


class AVAdapterError(RuntimeError):
    pass


_API = "https://www.alphavantage.co/query"


def _api_key() -> str:
    key = os.getenv("AKU_ALPHAVANTAGE_API_KEY") or os.getenv("ALPHAVANTAGE_API_KEY")
    if not key:
        raise AVAdapterError("Alpha Vantage API key missing. Set AKU_ALPHAVANTAGE_API_KEY or ALPHAVANTAGE_API_KEY")
    return key


def _get(params: Dict[str, Any]) -> Dict[str, Any]:
    q = dict(params)
    q["apikey"] = _api_key()
    url = f"{_API}?{parse.urlencode(q)}"
    with request.urlopen(url) as resp:
        data = resp.read()
    try:
        obj = json.loads(data)
    except Exception as exc:
        raise AVAdapterError("Invalid JSON from Alpha Vantage") from exc
    if isinstance(obj, dict) and ("Note" in obj or "Error Message" in obj):
        # Rate limit or error; return empty structure
        return obj
    return obj


def _parse_daily(obj: Dict[str, Any]) -> pd.DataFrame:
    # TIME_SERIES_DAILY or ADJUSTED
    key = next((k for k in obj.keys() if k.startswith("Time Series") or k.startswith("Monthly") or k.startswith("Weekly")), None)
    if not key:
        return pd.DataFrame([])
    ts = obj.get(key, {})
    rows = []
    for d, v in ts.items():
        rows.append({
            'date': d,
            'open': float(v.get('1. open', v.get('1. Open', 0)) or 0),
            'high': float(v.get('2. high', v.get('2. High', 0)) or 0),
            'low': float(v.get('3. low', v.get('3. Low', 0)) or 0),
            'close': float(v.get('4. close', v.get('4. Close', 0)) or 0),
            'volume': float(v.get('6. volume', v.get('5. volume', v.get('Volume', 0)) or 0)),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values('date')
    return df


def _parse_intraday(obj: Dict[str, Any]) -> pd.DataFrame:
    key = next((k for k in obj.keys() if k.startswith("Time Series")), None)
    if not key:
        return pd.DataFrame([])
    ts = obj.get(key, {})
    rows = []
    for d, v in ts.items():
        rows.append({
            'datetime': d,
            'open': float(v.get('1. open', 0) or 0),
            'high': float(v.get('2. high', 0) or 0),
            'low': float(v.get('3. low', 0) or 0),
            'close': float(v.get('4. close', 0) or 0),
            'volume': float(v.get('5. volume', 0) or 0),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values('datetime')
    return df


def _parse_global_quote(obj: Dict[str, Any], symbol: str) -> pd.DataFrame:
    q = obj.get('Global Quote') or obj.get('Realtime Global Securities Quote') or {}
    if not isinstance(q, dict) or not q:
        return pd.DataFrame([])
    last = q.get('05. price') or q.get('c')
    prev = q.get('08. previous close') or q.get('pc')
    try:
        last_f = float(last) if last is not None else None
    except Exception:
        last_f = None
    try:
        prev_f = float(prev) if prev is not None else None
    except Exception:
        prev_f = None
    change = (last_f - prev_f) if (last_f is not None and prev_f is not None) else None
    pct = (change / prev_f * 100.0) if (change is not None and prev_f) else None
    return pd.DataFrame([{
        'symbol': symbol,
        'last': last_f,
        'prev_close': prev_f,
        'change': change,
        'pct_change': pct,
    }])


def _parse_series(obj: Dict[str, Any]) -> pd.DataFrame:
    data = obj.get('data') if isinstance(obj, dict) else None
    if not isinstance(data, list):
        return pd.DataFrame([])
    rows = []
    for it in data:
        rows.append({'date': it.get('date'), 'value': (None if it.get('value') in (None, '', 'NaN') else float(it.get('value')))})
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values('date')
    return df


def _parse_overview(obj: Dict[str, Any], symbol: str) -> pd.DataFrame:
    if not isinstance(obj, dict) or not obj:
        return pd.DataFrame([])
    out = {k.lower().replace(' ', '_'): v for k, v in obj.items()}
    out['symbol'] = symbol
    return pd.DataFrame([out])


def _parse_statement(obj: Dict[str, Any], key: str, symbol: str) -> pd.DataFrame:
    # key one of 'annualReports', 'quarterlyReports'
    arr = obj.get(key) or []
    if not isinstance(arr, list) or not arr:
        return pd.DataFrame([])
    rows = []
    for it in arr:
        rec = {kk.lower().replace(' ', '_'): it.get(kk) for kk in it.keys()}
        rec['symbol'] = symbol
        rec['period'] = 'annual' if key.startswith('annual') else 'quarterly'
        rows.append(rec)
    df = pd.DataFrame(rows)
    # try convert common numerics
    for col in df.columns:
        if col not in {'symbol', 'period', 'fiscaldateending', 'reportedcurrency'}:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except Exception:
                pass
    return df


def call_alphavantage(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    # US/HK daily
    if dataset_id.endswith('ohlcv_daily.av'):
        symbol = (params.get('symbol') or '').upper()
        func = 'TIME_SERIES_DAILY_ADJUSTED'
        obj = _get({'function': func, 'symbol': symbol, 'outputsize': 'full'})
        df = _parse_daily(obj)
        if not df.empty:
            df.insert(0, 'symbol', symbol)
        return (func, df)
    # US/HK minute
    if dataset_id.endswith('ohlcv_min.av'):
        symbol = (params.get('symbol') or '').upper()
        freq = str(params.get('freq') or 'min5').lower()
        interval = {'min1':'1min','1':'1min','min5':'5min','5':'5min','min15':'15min','15':'15min','min30':'30min','30':'30min','min60':'60min','60':'60min'}.get(freq, '5min')
        func = 'TIME_SERIES_INTRADAY'
        obj = _get({'function': func, 'symbol': symbol, 'interval': interval, 'outputsize': 'full'})
        df = _parse_intraday(obj)
        if not df.empty:
            df.insert(0, 'symbol', symbol)
        return (f'{func}_{interval}', df)
    # Quote
    if dataset_id.endswith('quote.av'):
        symbol = (params.get('symbol') or '').upper()
        func = 'GLOBAL_QUOTE'
        obj = _get({'function': func, 'symbol': symbol})
        df = _parse_global_quote(obj, symbol)
        return (func, df)
    # Macro US series
    if 'macro.us.cpi' in dataset_id:
        func = 'CPI'
        obj = _get({'function': func})
        return (func, _parse_series(obj))
    if 'macro.us.ppi' in dataset_id:
        func = 'PPI'
        obj = _get({'function': func})
        return (func, _parse_series(obj))
    if 'macro.us.pmi' in dataset_id:
        func = 'PMI'
        obj = _get({'function': func})
        return (func, _parse_series(obj))
    if 'macro.us.gdp' in dataset_id:
        func = 'REAL_GDP'
        obj = _get({'function': func})
        return (func, _parse_series(obj))
    if 'macro.us.unemployment' in dataset_id:
        func = 'UNEMPLOYMENT'
        obj = _get({'function': func})
        return (func, _parse_series(obj))
    # Fundamentals - Overview
    if dataset_id.endswith('fundamentals.overview.av'):
        symbol = (params.get('symbol') or '').upper()
        func = 'OVERVIEW'
        obj = _get({'function': func, 'symbol': symbol})
        return (func, _parse_overview(obj, symbol))
    # Fundamentals - Income Statement (annual/quarterly combined; filter by period param if provided)
    if dataset_id.endswith('fundamentals.income_statement.av'):
        symbol = (params.get('symbol') or '').upper()
        func = 'INCOME_STATEMENT'
        obj = _get({'function': func, 'symbol': symbol})
        period = (params.get('period') or '').lower()  # 'annual'|'quarterly'|''
        frames = []
        if period in ('', 'annual'):
            frames.append(_parse_statement(obj, 'annualReports', symbol))
        if period in ('', 'quarterly'):
            frames.append(_parse_statement(obj, 'quarterlyReports', symbol))
        df = pd.concat([f for f in frames if f is not None and not f.empty], ignore_index=True) if frames else pd.DataFrame([])
        return (func, df)
    # Fundamentals - Balance Sheet
    if dataset_id.endswith('fundamentals.balance_sheet.av'):
        symbol = (params.get('symbol') or '').upper()
        func = 'BALANCE_SHEET'
        obj = _get({'function': func, 'symbol': symbol})
        period = (params.get('period') or '').lower()
        frames = []
        if period in ('', 'annual'):
            frames.append(_parse_statement(obj, 'annualReports', symbol))
        if period in ('', 'quarterly'):
            frames.append(_parse_statement(obj, 'quarterlyReports', symbol))
        df = pd.concat([f for f in frames if f is not None and not f.empty], ignore_index=True) if frames else pd.DataFrame([])
        return (func, df)
    # Fundamentals - Cash Flow
    if dataset_id.endswith('fundamentals.cash_flow.av'):
        symbol = (params.get('symbol') or '').upper()
        func = 'CASH_FLOW'
        obj = _get({'function': func, 'symbol': symbol})
        period = (params.get('period') or '').lower()
        frames = []
        if period in ('', 'annual'):
            frames.append(_parse_statement(obj, 'annualReports', symbol))
        if period in ('', 'quarterly'):
            frames.append(_parse_statement(obj, 'quarterlyReports', symbol))
        df = pd.concat([f for f in frames if f is not None and not f.empty], ignore_index=True) if frames else pd.DataFrame([])
        return (func, df)
    # Fundamentals - Earnings (contains annual and quarterly EPS series)
    if dataset_id.endswith('fundamentals.earnings.av'):
        symbol = (params.get('symbol') or '').upper()
        func = 'EARNINGS'
        obj = _get({'function': func, 'symbol': symbol})
        # flatten annual and quarterly EPS
        rows = []
        for it in (obj.get('annualEarnings') or []):
            rows.append({'symbol': symbol, 'period': 'annual', **{k.lower(): it.get(k) for k in it}})
        for it in (obj.get('quarterlyEarnings') or []):
            rows.append({'symbol': symbol, 'period': 'quarterly', **{k.lower(): it.get(k) for k in it}})
        df = pd.DataFrame(rows)
        return (func, df)
    return ('alphavantage.unsupported', pd.DataFrame([]))