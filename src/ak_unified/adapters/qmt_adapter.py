from __future__ import annotations

import platform
from typing import Any, Dict, Tuple

import pandas as pd


class QmtAdapterError(RuntimeError):
    pass


def _ensure_windows() -> None:
    if platform.system().lower() != 'windows':
        raise QmtAdapterError(
            "QMT adapter is Windows-only. Please run on Windows with QMT/ThinkTrader client and native API installed."
        )


def call_qmt(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    # NOTE: QMT native API docs: https://dict.thinktrader.net/dictionary/ and https://dict.thinktrader.net/nativeApi/start_now.html
    # This adapter is a placeholder designed to align with our unified schema. It requires Windows and QMT native API.
    _ensure_windows()
    # TODO: integrate QMT Python bindings to fetch real data. Below are placeholders for structure alignment.
    if dataset_id.endswith('ohlcv_daily'):
        # Expected params: symbol, start, end
        df = pd.DataFrame(columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        return ('qmt.ohlcv_daily', df)
    if dataset_id.endswith('ohlcv_min'):
        # Expected params: symbol, start, end, freq
        df = pd.DataFrame(columns=['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        return ('qmt.ohlcv_min', df)
    if dataset_id.endswith('quote'):
        # Realtime quote snapshot
        df = pd.DataFrame(columns=['symbol', 'symbol_name', 'datetime', 'last', 'open', 'high', 'low', 'prev_close', 'volume', 'amount'])
        return ('qmt.realtime_quote', df)
    if 'calendar' in dataset_id:
        df = pd.DataFrame(columns=['date', 'is_trading_day', 'market'])
        return ('qmt.calendar', df)
    if 'adjust_factor' in dataset_id:
        df = pd.DataFrame(columns=['symbol', 'date', 'adjust_factor'])
        return ('qmt.adjust_factor', df)
    return ('qmt.unsupported', pd.DataFrame([]))