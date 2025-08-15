from __future__ import annotations

from typing import Any, Dict, Tuple, Optional

import pandas as pd


class YFAdapterError(RuntimeError):
    pass


def _import_yf():
    try:
        import yfinance as yf  # type: ignore
        return yf
    except Exception as exc:
        raise YFAdapterError("Failed to import yfinance. Install with pip install yfinance") from exc


def _norm_symbol_us(symbol: str) -> str:
    return symbol.strip().upper()


def _norm_symbol_hk(symbol: str) -> str:
    s = symbol.upper().replace('.HK', '').strip()
    s = s.zfill(4)
    return f"{s}.HK"


def _to_records(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    out = df.reset_index().rename(columns={time_col: 'datetime' if time_col == 'Datetime' else 'date',
                                           'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
    # amount not provided by yfinance
    return out


def call_yfinance(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    yf = _import_yf()
    # US daily
    if dataset_id.endswith('us.ohlcv_daily.yf'):
        symbol = _norm_symbol_us(params.get('symbol') or '')
        start = params.get('start')
        end = params.get('end')
        try:
            df = yf.download(symbol, start=start, end=end, interval='1d', auto_adjust=False, progress=False)
        except Exception as exc:
            raise YFAdapterError(str(exc)) from exc
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = _to_records(df, 'Date')
            df.insert(0, 'symbol', symbol)
        return ('yfinance.download_1d', df if isinstance(df, pd.DataFrame) else pd.DataFrame([]))
    # HK daily
    if dataset_id.endswith('hk.ohlcv_daily.yf'):
        raw = params.get('symbol') or ''
        symbol = _norm_symbol_hk(raw)
        start = params.get('start')
        end = params.get('end')
        try:
            df = yf.download(symbol, start=start, end=end, interval='1d', auto_adjust=False, progress=False)
        except Exception as exc:
            raise YFAdapterError(str(exc)) from exc
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = _to_records(df, 'Date')
            df.insert(0, 'symbol', symbol)
        return ('yfinance.download_1d', df if isinstance(df, pd.DataFrame) else pd.DataFrame([]))
    # US/HK minute (best effort; yfinance limits period)
    if dataset_id.endswith('ohlcv_min.yf'):
        raw = params.get('symbol') or ''
        symbol = _norm_symbol_us(raw) if 'us.' in dataset_id else _norm_symbol_hk(raw)
        freq = str(params.get('freq') or 'min5').lower()
        interval = {'min1': '1m', '1': '1m', 'min5': '5m', '5': '5m', 'min15': '15m', '15': '15m', 'min30': '30m', '30': '30m', 'min60': '60m', '60': '60m'}.get(freq, '5m')
        # yfinance requires a period for intraday; use max=60d for <=60m interval
        try:
            df = yf.download(symbol, period='60d', interval=interval, auto_adjust=False, progress=False)
        except Exception as exc:
            raise YFAdapterError(str(exc)) from exc
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = _to_records(df, 'Datetime')
            df.insert(0, 'symbol', symbol)
        return (f'yfinance.download_{interval}', df if isinstance(df, pd.DataFrame) else pd.DataFrame([]))
    # Quotes (last trade and basic fields)
    if dataset_id.endswith('quote.yf'):
        raw = params.get('symbols')
        symbols = raw if isinstance(raw, list) and raw else None
        # yfinance does not have a single endpoint for multi-symbol real-time; use download with period=1d last row as proxy
        try:
            if symbols and len(symbols) > 1:
                df = yf.download(symbols, period='1d', interval='1d', auto_adjust=False, progress=False, group_by='ticker')
                rows = []
                for sym in symbols:
                    sub = df[sym] if sym in df else pd.DataFrame([])
                    if isinstance(sub, pd.DataFrame) and not sub.empty:
                        rec = _to_records(sub.tail(1), 'Date')
                        rec.insert(0, 'symbol', sym)
                        rows.append(rec)
                out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame([])
                return ('yfinance.download_quote_multi', out)
            else:
                sym = symbols[0] if symbols else (params.get('symbol') or '')
                sym = _norm_symbol_us(sym) if sym and sym.find('.') == -1 else sym
                tkr = yf.Ticker(sym)
                info = tkr.fast_info if hasattr(tkr, 'fast_info') else {}
                last = getattr(info, 'last_price', None) if not isinstance(info, dict) else info.get('last_price')
                prev = getattr(info, 'previous_close', None) if not isinstance(info, dict) else info.get('previous_close')
                change = (float(last) - float(prev)) if (last is not None and prev is not None) else None
                pct = (change / float(prev) * 100.0) if (change is not None and prev) else None
                out = pd.DataFrame([{
                    'symbol': sym,
                    'last': float(last) if last is not None else None,
                    'prev_close': float(prev) if prev is not None else None,
                    'change': change,
                    'pct_change': pct,
                }])
                return ('yfinance.ticker_fast_info', out)
        except Exception as exc:
            raise YFAdapterError(str(exc)) from exc
    return ('yfinance.unsupported', pd.DataFrame([]))