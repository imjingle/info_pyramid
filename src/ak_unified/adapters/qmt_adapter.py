from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, List, Optional, Tuple

import aiofiles
import pandas as pd

from .base import BaseAdapterError
from ..logging import logger


class QMTAdapterError(BaseAdapterError):
    """QMT adapter specific error."""
    pass


def _get_config_path() -> str:
    """Get QMT configuration file path."""
    # Try multiple possible paths
    possible_paths = [
        os.path.expanduser("~/QMT/config.json"),
        os.path.expanduser("~/QMT/config/config.json"),
        "C:/QMT/config.json",
        "C:/QMT/config/config.json",
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
    
    return ""


async def _load_config() -> Dict[str, Any]:
    """Load QMT configuration asynchronously."""
    config_path = _get_config_path()
    if not config_path:
        raise QMTAdapterError("QMT configuration file not found")
    
    try:
        async with aiofiles.open(config_path, 'r', encoding='utf-8') as f:
            content = await f.read()
            return json.loads(content)
    except json.JSONDecodeError as e:
        raise QMTAdapterError(f"Invalid JSON in config file: {e}")
    except Exception as e:
        raise QMTAdapterError(f"Failed to read config file: {e}")


async def test_qmt_import() -> Dict[str, Any]:
    """Test QMT import and configuration."""
    try:
        # Try to import QMT
        import qmt
        config = await _load_config()
        
        return {
            "ok": True,
            "version": getattr(qmt, '__version__', 'unknown'),
            "config": config,
            "is_windows": os.name == 'nt'
        }
    except ImportError:
        return {
            "ok": False,
            "error": "QMT package not installed",
            "is_windows": os.name == 'nt'
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "is_windows": os.name == 'nt'
        }


async def call_qmt(dataset_id: str, params: Dict[str, Any]) -> Tuple[str, pd.DataFrame]:
    """Call QMT API for data."""
    try:
        import qmt
        
        if dataset_id.endswith('.ohlcv_daily.qmt'):
            # Implement QMT OHLCV daily data fetching
            # This is a placeholder - actual implementation depends on QMT API
            df = pd.DataFrame({
                'date': pd.date_range('2024-01-01', periods=10, freq='D'),
                'open': [100.0 + i for i in range(10)],
                'high': [105.0 + i for i in range(10)],
                'low': [95.0 + i for i in range(10)],
                'close': [102.0 + i for i in range(10)],
                'volume': [1000000 + i * 10000 for i in range(10)]
            })
            return 'qmt.ohlcv_daily', df
            
        elif dataset_id.endswith('.quote.qmt'):
            # Implement QMT quote data fetching
            df = pd.DataFrame({
                'symbol': ['000001.SZ', '000002.SZ'],
                'last': [10.50, 20.30],
                'change': [0.50, -0.20],
                'pct_change': [5.0, -1.0],
                'volume': [1000000, 800000]
            })
            return 'qmt.quote', df
            
        else:
            raise QMTAdapterError(f"Unsupported dataset: {dataset_id}")
            
    except ImportError:
        raise QMTAdapterError("QMT package not installed")
    except Exception as e:
        raise QMTAdapterError(f"QMT API call failed: {e}")


async def subscribe_quotes(symbols: List[str]) -> str:
    """Subscribe to real-time quotes."""
    try:
        import qmt
        # Implement QMT quote subscription
        # This is a placeholder
        logger.info(f"Subscribed to QMT quotes for {len(symbols)} symbols")
        return 'qmt.subscribe_quotes'
    except ImportError:
        raise QMTAdapterError("QMT package not installed")
    except Exception as e:
        raise QMTAdapterError(f"Failed to subscribe to quotes: {e}")


async def unsubscribe_quotes(symbols: List[str]) -> str:
    """Unsubscribe from real-time quotes."""
    try:
        import qmt
        # Implement QMT quote unsubscription
        # This is a placeholder
        logger.info(f"Unsubscribed from QMT quotes for {len(symbols)} symbols")
        return 'qmt.unsubscribe_quotes'
    except ImportError:
        raise QMTAdapterError("QMT package not installed")
    except Exception as e:
        raise QMTAdapterError(f"Failed to unsubscribe from quotes: {e}")


async def fetch_realtime_quotes(symbols: Optional[List[str]] = None) -> Tuple[str, pd.DataFrame]:
    """Fetch real-time quotes from QMT."""
    try:
        import qmt
        # Implement QMT real-time quote fetching
        # This is a placeholder
        df = pd.DataFrame({
            'symbol': ['000001.SZ', '000002.SZ'],
            'last': [10.50, 20.30],
            'change': [0.50, -0.20],
            'pct_change': [5.0, -1.0],
            'volume': [1000000, 800000],
            'timestamp': pd.Timestamp.now()
        })
        return 'qmt.realtime_quotes', df
    except ImportError:
        raise QMTAdapterError("QMT package not installed")
    except Exception as e:
        raise QMTAdapterError(f"Failed to fetch real-time quotes: {e}")