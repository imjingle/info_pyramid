"""
Earnings Calendar Adapter

Provides access to earnings calendar data including:
- Earnings release dates
- Earnings forecasts
- Important announcements
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .base import BaseAdapterError
from .akshare_adapter import call_akshare
from .alphavantage_adapter import call_alphavantage
from .yfinance_adapter import call_yfinance
from ..rate_limiter import acquire_rate_limit
from ..logging import logger


class EarningsCalendarError(BaseAdapterError):
    """Earnings calendar specific error."""
    pass


class EarningsCalendarAdapter:
    """Adapter for earnings calendar data from multiple sources."""
    
    def __init__(self):
        self.supported_markets = ['cn', 'us', 'hk']
    
    async def get_earnings_calendar(
        self, 
        market: str = 'cn',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        symbols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Get earnings calendar for specified market and date range.
        
        Args:
            market: Market code ('cn' for China, 'us' for US, 'hk' for Hong Kong)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            symbols: Optional list of stock symbols to filter
            
        Returns:
            DataFrame with earnings calendar data
        """
        if market not in self.supported_markets:
            raise EarningsCalendarError(f"Unsupported market: {market}")
        
        if market == 'cn':
            return await self._get_cn_earnings_calendar(start_date, end_date, symbols)
        elif market == 'us':
            return await self._get_us_earnings_calendar(start_date, end_date, symbols)
        elif market == 'hk':
            return await self._get_hk_earnings_calendar(start_date, end_date, symbols)
        
        return pd.DataFrame()
    
    async def _get_cn_earnings_calendar(
        self, 
        start_date: Optional[str], 
        end_date: Optional[str],
        symbols: Optional[List[str]]
    ) -> pd.DataFrame:
        """Get China A-share earnings calendar."""
        try:
            # Try EastMoney first (most comprehensive)
            await acquire_rate_limit('akshare', 'eastmoney')
            
            # Get earnings calendar from EastMoney
            df = await call_akshare(
                ['stock_financial_report_em'],
                {},
                function_name='stock_financial_report_em'
            )
            
            if df.empty:
                return df
            
            # Filter by date range if specified
            if start_date:
                df = df[df['report_date'] >= start_date]
            if end_date:
                df = df[df['report_date'] <= end_date]
            
            # Filter by symbols if specified
            if symbols:
                df = df[df['symbol'].isin(symbols)]
            
            # Standardize column names
            df = self._standardize_cn_columns(df)
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get EastMoney earnings calendar: {e}")
            
            # Fallback to Baidu for report time
            try:
                await acquire_rate_limit('akshare', 'baidu')
                df = await call_akshare(
                    ['news_report_time_baidu'],
                    {},
                    function_name='news_report_time_baidu'
                )
                
                if not df.empty:
                    df = self._standardize_baidu_columns(df)
                    return df
                    
            except Exception as e2:
                logger.warning(f"Failed to get Baidu earnings calendar: {e2}")
        
        return pd.DataFrame()
    
    async def _get_us_earnings_calendar(
        self, 
        start_date: Optional[str], 
        end_date: Optional[str],
        symbols: Optional[List[str]]
    ) -> pd.DataFrame:
        """Get US market earnings calendar."""
        try:
            await acquire_rate_limit('akshare', 'eastmoney')
            
            # Get US earnings data from EastMoney
            df = await call_akshare(
                ['stock_financial_us_report_em'],
                {},
                function_name='stock_financial_us_report_em'
            )
            
            if not df.empty:
                # Filter by date range if specified
                if start_date:
                    df = df[df['report_date'] >= start_date]
                if end_date:
                    df = df[df['report_date'] <= end_date]
                
                # Filter by symbols if specified
                if symbols:
                    df = df[df['symbol'].isin(symbols)]
                
                df = self._standardize_us_columns(df)
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get US earnings calendar from EastMoney: {e}")
            
            # Fallback to Alpha Vantage
            try:
                await acquire_rate_limit('alphavantage', 'default')
                
                df = await call_alphavantage(
                    'EARNINGS_CALENDAR',
                    {
                        'horizon': '3month',
                        'symbol': symbols[0] if symbols else None
                    }
                )
                
                if not df.empty:
                    df = self._standardize_us_columns(df)
                    return df
                    
            except Exception as e2:
                logger.warning(f"Failed to get Alpha Vantage earnings calendar: {e2}")
            
            # Fallback to Yahoo Finance for individual symbols
            if symbols:
                results = []
                for symbol in symbols[:10]:  # Limit to 10 symbols to avoid rate limiting
                    try:
                        await acquire_rate_limit('yfinance', 'default')
                        df = await call_yfinance(
                            ['earnings_dates'],
                            {'symbol': symbol},
                            function_name='earnings_dates'
                        )
                        
                        if not df.empty:
                            df['symbol'] = symbol
                            results.append(df)
                            
                    except Exception as e3:
                        logger.warning(f"Failed to get Yahoo Finance earnings for {symbol}: {e3}")
                        continue
                
                if results:
                    combined_df = pd.concat(results, ignore_index=True)
                    return self._standardize_us_columns(combined_df)
        
        return pd.DataFrame()
    
    async def _get_hk_earnings_calendar(
        self, 
        start_date: Optional[str], 
        end_date: Optional[str],
        symbols: Optional[List[str]]
    ) -> pd.DataFrame:
        """Get Hong Kong market earnings calendar."""
        try:
            await acquire_rate_limit('akshare', 'eastmoney')
            
            # Get HK earnings data from EastMoney
            df = await call_akshare(
                ['stock_financial_hk_report_em'],
                {},
                function_name='stock_financial_hk_report_em'
            )
            
            if not df.empty:
                # Filter by date range if specified
                if start_date:
                    df = df[df['report_date'] >= start_date]
                if end_date:
                    df = df[df['report_date'] <= end_date]
                
                # Filter by symbols if specified
                if symbols:
                    df = df[df['symbol'].isin(symbols)]
                
                df = self._standardize_hk_columns(df)
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get HK earnings calendar: {e}")
            
            # Fallback to Baidu for report time
            try:
                await acquire_rate_limit('akshare', 'baidu')
                df = await call_akshare(
                    ['news_report_time_baidu'],
                    {'market': 'hk'},
                    function_name='news_report_time_baidu'
                )
                
                if not df.empty:
                    df = self._standardize_baidu_columns(df)
                    return df
                    
            except Exception as e2:
                logger.warning(f"Failed to get Baidu HK earnings calendar: {e2}")
        
        return pd.DataFrame()
    
    async def get_earnings_dates(self, symbol: str, market: str = 'cn') -> pd.DataFrame:
        """
        Get earnings dates for a specific symbol.
        
        Args:
            symbol: Stock symbol
            market: Market code
            
        Returns:
            DataFrame with earnings dates
        """
        if market == 'cn':
            return await self._get_cn_earnings_dates(symbol)
        elif market == 'us':
            return await self._get_us_earnings_dates(symbol)
        elif market == 'hk':
            return await self._get_hk_earnings_dates(symbol)
        
        return pd.DataFrame()
    
    async def _get_cn_earnings_dates(self, symbol: str) -> pd.DataFrame:
        """Get earnings dates for China A-share."""
        try:
            await acquire_rate_limit('akshare', 'eastmoney')
            
            df = await call_akshare(
                ['stock_financial_report_em'],
                {'symbol': symbol},
                function_name='stock_financial_report_em'
            )
            
            if not df.empty:
                df = self._standardize_cn_columns(df)
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get CN earnings dates for {symbol}: {e}")
        
        return pd.DataFrame()
    
    async def _get_us_earnings_dates(self, symbol: str) -> pd.DataFrame:
        """Get earnings dates for US stock."""
        try:
            await acquire_rate_limit('yfinance', 'default')
            
            df = await call_yfinance(
                ['earnings_dates'],
                {'symbol': symbol},
                function_name='earnings_dates'
            )
            
            if not df.empty:
                df['symbol'] = symbol
                df = self._standardize_us_columns(df)
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get US earnings dates for {symbol}: {e}")
        
        return pd.DataFrame()
    
    async def _get_hk_earnings_dates(self, symbol: str) -> pd.DataFrame:
        """Get earnings dates for Hong Kong stock."""
        # Similar to CN but with HK market filter
        return await self._get_cn_earnings_dates(symbol)
    
    async def get_earnings_forecast(
        self, 
        symbol: str, 
        market: str = 'cn',
        period: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get earnings forecast for a specific symbol.
        
        Args:
            symbol: Stock symbol
            market: Market code
            period: Forecast period (e.g., '2024', '2024Q1')
            
        Returns:
            DataFrame with earnings forecast data
        """
        if market == 'cn':
            return await self._get_cn_earnings_forecast(symbol, period)
        elif market == 'us':
            return await self._get_us_earnings_forecast(symbol, period)
        
        return pd.DataFrame()
    
    async def _get_cn_earnings_forecast(self, symbol: str, period: Optional[str]) -> pd.DataFrame:
        """Get earnings forecast for China A-share."""
        try:
            await acquire_rate_limit('akshare', 'eastmoney')
            
            # Get profit forecast
            df = await call_akshare(
                ['stock_profit_forecast'],
                {'symbol': symbol},
                function_name='stock_profit_forecast'
            )
            
            if not df.empty:
                df = self._standardize_forecast_columns(df)
                
                # Filter by period if specified
                if period:
                    df = df[df['forecast_period'] == period]
                
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get CN earnings forecast for {symbol}: {e}")
        
        return pd.DataFrame()
    
    async def _get_us_earnings_forecast(self, symbol: str, period: Optional[str]) -> pd.DataFrame:
        """Get earnings forecast for US stock."""
        try:
            await acquire_rate_limit('alphavantage', 'default')
            
            # Get earnings from Alpha Vantage
            df = await call_alphavantage(
                'EARNINGS',
                {'symbol': symbol}
            )
            
            if not df.empty:
                df = self._standardize_us_forecast_columns(df)
                
                # Filter by period if specified
                if period:
                    df = df[df['fiscalDateEnding'] == period]
                
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get US earnings forecast for {symbol}: {e}")
        
        return pd.DataFrame()
    
    def _standardize_cn_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for China market data."""
        column_mapping = {
            'symbol': 'symbol',
            'name': 'company_name',
            'report_date': 'report_date',
            'report_period': 'report_period',
            'report_type': 'report_type',
            'eps': 'eps',
            'eps_yoy': 'eps_yoy',
            'revenue': 'revenue',
            'revenue_yoy': 'revenue_yoy',
            'net_profit': 'net_profit',
            'net_profit_yoy': 'net_profit_yoy'
        }
        
        # Rename columns that exist
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        if existing_cols:
            df = df.rename(columns=existing_cols)
        
        # Ensure required columns exist
        required_cols = ['symbol', 'report_date', 'report_period', 'report_type']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None
        
        return df
    
    def _standardize_us_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for US market data."""
        column_mapping = {
            'symbol': 'symbol',
            'fiscalDateEnding': 'report_date',
            'fiscalPeriod': 'report_period',
            'estimatedEPS': 'eps_estimate',
            'reportedEPS': 'eps_actual',
            'estimatedRevenue': 'revenue_estimate',
            'reportedRevenue': 'revenue_actual',
            'surprise': 'surprise',
            'surprisePercentage': 'surprise_percentage'
        }
        
        # Rename columns that exist
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        if existing_cols:
            df = df.rename(columns=existing_cols)
        
        # Ensure required columns exist
        required_cols = ['symbol', 'report_date', 'report_period']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None
        
        return df
    
    def _standardize_hk_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for Hong Kong market data."""
        # Similar to CN but may have different column names
        return self._standardize_cn_columns(df)
    
    def _standardize_baidu_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for Baidu report time data."""
        column_mapping = {
            'symbol': 'symbol',
            'company_name': 'company_name',
            'report_date': 'report_date',
            'report_period': 'report_period',
            'report_type': 'report_type',
            'source': 'source'
        }
        
        # Rename columns that exist
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        if existing_cols:
            df = df.rename(columns=existing_cols)
        
        # Ensure required columns exist
        required_cols = ['symbol', 'report_date', 'report_period', 'report_type']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None
        
        return df
    
    def _standardize_forecast_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for forecast data."""
        column_mapping = {
            'symbol': 'symbol',
            'forecast_period': 'forecast_period',
            'forecast_type': 'forecast_type',
            'net_profit_change': 'net_profit_change',
            'change_reason': 'change_reason',
            'announcement_date': 'announcement_date'
        }
        
        # Rename columns that exist
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        if existing_cols:
            df = df.rename(columns=existing_cols)
        
        return df
    
    def _standardize_us_forecast_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for US forecast data."""
        column_mapping = {
            'symbol': 'symbol',
            'fiscalDateEnding': 'forecast_period',
            'estimatedEPS': 'eps_estimate',
            'reportedEPS': 'eps_actual',
            'estimatedRevenue': 'revenue_estimate',
            'reportedRevenue': 'revenue_actual'
        }
        
        # Rename columns that exist
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        if existing_cols:
            df = df.rename(columns=existing_cols)
        
        return df


# Convenience function for backward compatibility
async def call_earnings_calendar(
    function_name: str,
    params: Dict[str, Any],
    market: str = 'cn'
) -> Tuple[str, pd.DataFrame]:
    """
    Convenience function to call earnings calendar functions.
    
    Args:
        function_name: Function name to call
        params: Parameters for the function
        market: Market code
        
    Returns:
        Tuple of (function_name, DataFrame)
    """
    adapter = EarningsCalendarAdapter()
    
    if function_name == 'earnings_calendar':
        df = await adapter.get_earnings_calendar(
            market=params.get('market', 'cn'),
            start_date=params.get('start_date'),
            end_date=params.get('end_date'),
            symbols=params.get('symbols')
        )
    elif function_name == 'earnings_dates':
        df = await adapter.get_earnings_dates(
            symbol=params.get('symbol'),
            market=params.get('market', 'cn')
        )
    elif function_name == 'earnings_forecast':
        df = await adapter.get_earnings_forecast(
            symbol=params.get('symbol'),
            market=params.get('market', 'cn'),
            period=params.get('period')
        )
    else:
        raise EarningsCalendarError(f"Unknown function: {function_name}")
    
    return function_name, df