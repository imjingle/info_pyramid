from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional, Tuple

import aiohttp
import pandas as pd

from .base import BaseAdapterError
from ..rate_limiter import acquire_rate_limit, acquire_daily_rate_limit
from ..logging import logger


class AVAdapterError(BaseAdapterError):
    """Alpha Vantage adapter specific error."""
    pass


_API = "https://www.alphavantage.co/query"


def _api_key() -> str:
    from ..config import settings
    return settings.ALPHA_VANTAGE_API_KEY


async def _get(params: Dict[str, Any]) -> Dict[str, Any]:
    """Make async HTTP request to Alpha Vantage API."""
    q = dict(params)
    q["apikey"] = _api_key()
    url = f"{_API}?{q['apikey']}"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=q) as resp:
            if resp.status != 200:
                raise AVAdapterError(f"HTTP {resp.status}: {resp.reason}")
            data = await resp.text()
    
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
    
    # Convert keys to lowercase for consistency
    converted_obj = {}
    for k, v in obj.items():
        if isinstance(k, str):
            converted_obj[k.lower()] = v
        else:
            converted_obj[k] = v
    
    return pd.DataFrame([{
        'symbol': symbol,
        'name': converted_obj.get('name'),
        'description': converted_obj.get('description'),
        'exchange': converted_obj.get('exchange'),
        'currency': converted_obj.get('currency'),
        'country': converted_obj.get('country'),
        'sector': converted_obj.get('sector'),
        'industry': converted_obj.get('industry'),
        'market_cap': converted_obj.get('marketcapitalization'),
        'pe_ratio': converted_obj.get('peratio'),
        'dividend_yield': converted_obj.get('dividendyield'),
        'eps': converted_obj.get('eps'),
        'book_value': converted_obj.get('bookvalue'),
        'price_to_book': converted_obj.get('pricebook'),
        'ev_to_ebitda': converted_obj.get('evtoebitda'),
        'profit_margin': converted_obj.get('profitmargin'),
        'operating_margin': converted_obj.get('operatingmarginttm'),
        'roa': converted_obj.get('returnonequity'),
        'roe': converted_obj.get('returnonequity'),
        'revenue': converted_obj.get('revenue'),
        'revenue_per_share': converted_obj.get('revenuepershare'),
        'revenue_growth': converted_obj.get('revenuegrowth'),
        'gross_profit': converted_obj.get('grossprofit'),
        'ebitda': converted_obj.get('ebitda'),
        'net_income': converted_obj.get('netincometocommon'),
        'debt_to_equity': converted_obj.get('debttoequity'),
        'current_ratio': converted_obj.get('currentratio'),
        'beta': converted_obj.get('beta'),
        '52_week_high': converted_obj.get('52weekhigh'),
        '52_week_low': converted_obj.get('52weeklow'),
        '50_day_ma': converted_obj.get('50daymovingaverage'),
        '200_day_ma': converted_obj.get('200daymovingaverage'),
        'shares_outstanding': converted_obj.get('sharesoutstanding'),
        'float_shares': converted_obj.get('float'),
        'avg_volume': converted_obj.get('averagevolume'),
        'avg_volume_10d': converted_obj.get('averagevolume10days'),
        'short_ratio': converted_obj.get('shortratio'),
        'short_percent': converted_obj.get('shortpercentoffloat'),
        'insider_percent': converted_obj.get('insiderpercent'),
        'institutional_percent': converted_obj.get('institutionalpercent'),
        'analyst_rating': converted_obj.get('analysttargetprice'),
        'price_target': converted_obj.get('analysttargetprice'),
        'price_target_high': converted_obj.get('analysttargetprice'),
        'price_target_low': converted_obj.get('analysttargetprice'),
        'price_target_median': converted_obj.get('analysttargetprice'),
        'price_target_mean': converted_obj.get('analysttargetprice'),
        'price_target_count': converted_obj.get('analysttargetprice'),
        'last_updated': converted_obj.get('lastupdated'),
    }])


async def call_alphavantage(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    """Call Alpha Vantage API with rate limiting."""
    # Acquire rate limits
    await acquire_rate_limit('alphavantage')
    await acquire_daily_rate_limit('alphavantage')
    
    try:
        if dataset_id.endswith('.ohlcv_daily.av'):
            obj = await _get(params)
            df = _parse_daily(obj)
            return 'alphavantage.time_series_daily', df
        elif dataset_id.endswith('.ohlcv_intraday.av'):
            obj = await _get(params)
            df = _parse_intraday(obj)
            return 'alphavantage.time_series_intraday', df
        elif dataset_id.endswith('.quote.av'):
            obj = await _get(params)
            df = _parse_global_quote(obj, params.get('symbol', ''))
            return 'alphavantage.global_quote', df
        elif dataset_id.endswith('.overview.av'):
            obj = await _get(params)
            df = _parse_overview(obj, params.get('symbol', ''))
            return 'alphavantage.company_overview', df
        elif dataset_id.endswith('.series.av'):
            obj = await _get(params)
            df = _parse_series(obj)
            return 'alphavantage.time_series', df
        else:
            raise AVAdapterError(f"Unknown dataset: {dataset_id}")
    except Exception as e:
        logger.error(f"Alpha Vantage API error: {e}")
        raise AVAdapterError(f"API call failed: {e}") from e