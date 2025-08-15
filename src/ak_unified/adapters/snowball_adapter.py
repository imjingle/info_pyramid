"""
Snowball Adapter

Provides access to Snowball data including:
- Stock information and quotes
- Financial data and reports
- Research reports and analysis
- User discussions and sentiment
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .base import BaseAdapterError
from ..rate_limiter import acquire_rate_limit
from ..logging import logger


class SnowballAdapterError(BaseAdapterError):
    """Snowball adapter specific error."""
    pass


class SnowballAdapter:
    """Adapter for Snowball data from pysnowball library."""
    
    def __init__(self):
        self.supported_markets = ['cn', 'hk', 'us']
        self._snowball = None
    
    def _import_snowball(self):
        """Import pysnowball library."""
        try:
            import pysnowball as snowball
            return snowball
        except ImportError:
            raise SnowballAdapterError("Failed to import pysnowball. Please install: pip install pysnowball")
    
    async def get_stock_quote(
        self, 
        symbol: str, 
        market: str = 'cn'
    ) -> pd.DataFrame:
        """
        Get stock quote from Snowball.
        
        Args:
            symbol: Stock symbol
            market: Market code ('cn' for China, 'hk' for Hong Kong, 'us' for US)
            
        Returns:
            DataFrame with stock quote data
        """
        try:
            await acquire_rate_limit('snowball', 'default')
            
            snowball = self._import_snowball()
            
            # Get stock quote
            quote_data = snowball.quote_detail(symbol)
            
            if not quote_data or 'data' not in quote_data:
                return pd.DataFrame()
            
            data = quote_data['data']
            
            # Convert to DataFrame
            df = pd.DataFrame([{
                'symbol': symbol,
                'market': market,
                'name': data.get('name', ''),
                'current': data.get('current', 0),
                'change': data.get('change', 0),
                'change_percent': data.get('change_percent', 0),
                'open': data.get('open', 0),
                'high': data.get('high', 0),
                'low': data.get('low', 0),
                'volume': data.get('volume', 0),
                'market_cap': data.get('market_cap', 0),
                'pe_ratio': data.get('pe_ratio', 0),
                'pb_ratio': data.get('pb_ratio', 0),
                'dividend_yield': data.get('dividend_yield', 0),
                'timestamp': datetime.now()
            }])
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get Snowball stock quote for {symbol}: {e}")
            return pd.DataFrame()
    
    async def get_stock_financial_data(
        self, 
        symbol: str, 
        market: str = 'cn',
        period: str = 'annual'
    ) -> pd.DataFrame:
        """
        Get stock financial data from Snowball.
        
        Args:
            symbol: Stock symbol
            market: Market code
            period: Period type ('annual' or 'quarterly')
            
        Returns:
            DataFrame with financial data
        """
        try:
            await acquire_rate_limit('snowball', 'default')
            
            snowball = self._import_snowball()
            
            # Get financial data
            if period == 'quarterly':
                financial_data = snowball.financial_report(symbol, 'Q')
            else:
                financial_data = snowball.financial_report(symbol, 'Y')
            
            if not financial_data or 'data' not in financial_data:
                return pd.DataFrame()
            
            data = financial_data['data']
            
            # Convert to DataFrame
            rows = []
            for item in data:
                rows.append({
                    'symbol': symbol,
                    'market': market,
                    'period': item.get('period', ''),
                    'report_date': item.get('report_date', ''),
                    'revenue': item.get('revenue', 0),
                    'net_profit': item.get('net_profit', 0),
                    'eps': item.get('eps', 0),
                    'roe': item.get('roe', 0),
                    'roa': item.get('roa', 0),
                    'gross_margin': item.get('gross_margin', 0),
                    'net_margin': item.get('net_margin', 0),
                    'debt_ratio': item.get('debt_ratio', 0),
                    'current_ratio': item.get('current_ratio', 0),
                    'timestamp': datetime.now()
                })
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.warning(f"Failed to get Snowball financial data for {symbol}: {e}")
            return pd.DataFrame()
    
    async def get_stock_research_reports(
        self, 
        symbol: str, 
        market: str = 'cn',
        limit: int = 20
    ) -> pd.DataFrame:
        """
        Get stock research reports from Snowball.
        
        Args:
            symbol: Stock symbol
            market: Market code
            limit: Number of reports to return
            
        Returns:
            DataFrame with research report data
        """
        try:
            await acquire_rate_limit('snowball', 'default')
            
            snowball = self._import_snowball()
            
            # Get research reports
            reports_data = snowball.research_report(symbol, limit)
            
            if not reports_data or 'data' not in reports_data:
                return pd.DataFrame()
            
            data = reports_data['data']
            
            # Convert to DataFrame
            rows = []
            for item in data:
                rows.append({
                    'symbol': symbol,
                    'market': market,
                    'title': item.get('title', ''),
                    'author': item.get('author', ''),
                    'institution': item.get('institution', ''),
                    'publish_date': item.get('publish_date', ''),
                    'rating': item.get('rating', ''),
                    'target_price': item.get('target_price', 0),
                    'summary': item.get('summary', ''),
                    'url': item.get('url', ''),
                    'timestamp': datetime.now()
                })
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.warning(f"Failed to get Snowball research reports for {symbol}: {e}")
            return pd.DataFrame()
    
    async def get_stock_sentiment(
        self, 
        symbol: str, 
        market: str = 'cn',
        days: int = 7
    ) -> pd.DataFrame:
        """
        Get stock sentiment data from Snowball.
        
        Args:
            symbol: Stock symbol
            market: Market code
            days: Number of days to analyze
            
        Returns:
            DataFrame with sentiment data
        """
        try:
            await acquire_rate_limit('snowball', 'default')
            
            snowball = self._import_snowball()
            
            # Get sentiment data
            sentiment_data = snowball.sentiment(symbol, days)
            
            if not sentiment_data or 'data' not in sentiment_data:
                return pd.DataFrame()
            
            data = sentiment_data['data']
            
            # Convert to DataFrame
            rows = []
            for item in data:
                rows.append({
                    'symbol': symbol,
                    'market': market,
                    'date': item.get('date', ''),
                    'positive_count': item.get('positive_count', 0),
                    'negative_count': item.get('negative_count', 0),
                    'neutral_count': item.get('neutral_count', 0),
                    'sentiment_score': item.get('sentiment_score', 0),
                    'discussion_count': item.get('discussion_count', 0),
                    'timestamp': datetime.now()
                })
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.warning(f"Failed to get Snowball sentiment data for {symbol}: {e}")
            return pd.DataFrame()
    
    async def get_stock_discussions(
        self, 
        symbol: str, 
        market: str = 'cn',
        limit: int = 50
    ) -> pd.DataFrame:
        """
        Get stock discussions from Snowball.
        
        Args:
            symbol: Stock symbol
            market: Market code
            limit: Number of discussions to return
            
        Returns:
            DataFrame with discussion data
        """
        try:
            await acquire_rate_limit('snowball', 'default')
            
            snowball = self._import_snowball()
            
            # Get discussions
            discussions_data = snowball.discussion(symbol, limit)
            
            if not discussions_data or 'data' not in discussions_data:
                return pd.DataFrame()
            
            data = discussions_data['data']
            
            # Convert to DataFrame
            rows = []
            for item in data:
                rows.append({
                    'symbol': symbol,
                    'market': market,
                    'title': item.get('title', ''),
                    'content': item.get('content', ''),
                    'author': item.get('author', ''),
                    'publish_time': item.get('publish_time', ''),
                    'like_count': item.get('like_count', 0),
                    'comment_count': item.get('comment_count', 0),
                    'share_count': item.get('share_count', 0),
                    'sentiment': item.get('sentiment', ''),
                    'url': item.get('url', ''),
                    'timestamp': datetime.now()
                })
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.warning(f"Failed to get Snowball discussions for {symbol}: {e}")
            return pd.DataFrame()
    
    async def get_market_overview(
        self, 
        market: str = 'cn'
    ) -> pd.DataFrame:
        """
        Get market overview from Snowball.
        
        Args:
            market: Market code
            
        Returns:
            DataFrame with market overview data
        """
        try:
            await acquire_rate_limit('snowball', 'default')
            
            snowball = self._import_snowball()
            
            # Get market overview
            if market == 'cn':
                overview_data = snowball.market_overview('CN')
            elif market == 'hk':
                overview_data = snowball.market_overview('HK')
            elif market == 'us':
                overview_data = snowball.market_overview('US')
            else:
                return pd.DataFrame()
            
            if not overview_data or 'data' not in overview_data:
                return pd.DataFrame()
            
            data = overview_data['data']
            
            # Convert to DataFrame
            df = pd.DataFrame([{
                'market': market,
                'index_name': data.get('index_name', ''),
                'current_value': data.get('current_value', 0),
                'change': data.get('change', 0),
                'change_percent': data.get('change_percent', 0),
                'volume': data.get('volume', 0),
                'turnover': data.get('turnover', 0),
                'advance_count': data.get('advance_count', 0),
                'decline_count': data.get('decline_count', 0),
                'flat_count': data.get('flat_count', 0),
                'timestamp': datetime.now()
            }])
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get Snowball market overview for {market}: {e}")
            return pd.DataFrame()


# Convenience function for backward compatibility
async def call_snowball(
    function_name: str,
    params: Dict[str, Any],
    market: str = 'cn'
) -> Tuple[str, pd.DataFrame]:
    """
    Convenience function to call Snowball functions.
    
    Args:
        function_name: Function name to call
        params: Parameters for the function
        market: Market code
        
    Returns:
        Tuple of (function_name, DataFrame)
    """
    adapter = SnowballAdapter()
    
    if function_name == 'stock_quote':
        df = await adapter.get_stock_quote(
            symbol=params.get('symbol'),
            market=params.get('market', market)
        )
    elif function_name == 'financial_data':
        df = await adapter.get_stock_financial_data(
            symbol=params.get('symbol'),
            market=params.get('market', market),
            period=params.get('period', 'annual')
        )
    elif function_name == 'research_reports':
        df = await adapter.get_stock_research_reports(
            symbol=params.get('symbol'),
            market=params.get('market', market),
            limit=params.get('limit', 20)
        )
    elif function_name == 'sentiment':
        df = await adapter.get_stock_sentiment(
            symbol=params.get('symbol'),
            market=params.get('market', market),
            days=params.get('days', 7)
        )
    elif function_name == 'discussions':
        df = await adapter.get_stock_discussions(
            symbol=params.get('symbol'),
            market=params.get('market', market),
            limit=params.get('limit', 50)
        )
    elif function_name == 'market_overview':
        df = await adapter.get_market_overview(
            market=params.get('market', market)
        )
    else:
        raise SnowballAdapterError(f"Unknown function: {function_name}")
    
    return function_name, df