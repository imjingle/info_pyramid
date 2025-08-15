"""
Fund Portfolio Adapter

Provides access to fund portfolio data including:
- Fund holdings
- Holdings changes
- Portfolio analysis
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .base import BaseAdapterError
from .akshare_adapter import call_akshare
from ..rate_limiter import acquire_rate_limit
from ..logging import logger


class FundPortfolioError(BaseAdapterError):
    """Fund portfolio specific error."""
    pass


class FundPortfolioAdapter:
    """Adapter for fund portfolio data from multiple sources."""
    
    def __init__(self):
        self.supported_markets = ['cn', 'hk', 'us']
    
    async def get_fund_portfolio(
        self, 
        fund_code: str,
        market: str = 'cn',
        report_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get fund portfolio holdings.
        
        Args:
            fund_code: Fund code
            market: Market code ('cn' for China, 'hk' for Hong Kong, 'us' for US)
            report_date: Report date in YYYY-MM-DD format (None for latest)
            
        Returns:
            DataFrame with fund portfolio data
        """
        if market not in self.supported_markets:
            raise FundPortfolioError(f"Unsupported market: {market}")
        
        if market == 'cn':
            return await self._get_cn_fund_portfolio(fund_code, report_date)
        elif market == 'hk':
            return await self._get_hk_fund_portfolio(fund_code, report_date)
        elif market == 'us':
            return await self._get_us_fund_portfolio(fund_code, report_date)
        
        return pd.DataFrame()
    
    async def _get_cn_fund_portfolio(
        self, 
        fund_code: str, 
        report_date: Optional[str]
    ) -> pd.DataFrame:
        """Get China fund portfolio holdings."""
        try:
            await acquire_rate_limit('akshare', 'eastmoney')
            
            # Get fund portfolio holdings from EastMoney
            df = await call_akshare(
                ['fund_portfolio_hold_em'],
                {'fund': fund_code},
                function_name='fund_portfolio_hold_em'
            )
            
            if df.empty:
                return df
            
            # Filter by report date if specified
            if report_date:
                df = df[df['报告期'] == report_date]
            
            # Standardize column names
            df = self._standardize_cn_portfolio_columns(df)
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get EastMoney fund portfolio for {fund_code}: {e}")
        
        return pd.DataFrame()
    
    async def _get_hk_fund_portfolio(
        self, 
        fund_code: str, 
        report_date: Optional[str]
    ) -> pd.DataFrame:
        """Get Hong Kong fund portfolio holdings."""
        # HK fund data is limited, try to get from available sources
        try:
            await acquire_rate_limit('akshare', 'eastmoney')
            
            # Try to get HK fund data if available
            df = await call_akshare(
                ['fund_portfolio_hold_em'],
                {'fund': fund_code, 'market': 'hk'},
                function_name='fund_portfolio_hold_em'
            )
            
            if not df.empty:
                df = self._standardize_hk_portfolio_columns(df)
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get HK fund portfolio for {fund_code}: {e}")
        
        return pd.DataFrame()
    
    async def _get_us_fund_portfolio(
        self, 
        fund_code: str, 
        report_date: Optional[str]
    ) -> pd.DataFrame:
        """Get US fund portfolio holdings."""
        # US fund data is limited, try to get from available sources
        try:
            await acquire_rate_limit('akshare', 'eastmoney')
            
            # Try to get US fund data if available
            df = await call_akshare(
                ['fund_portfolio_hold_em'],
                {'fund': fund_code, 'market': 'us'},
                function_name='fund_portfolio_hold_em'
            )
            
            if not df.empty:
                df = self._standardize_us_portfolio_columns(df)
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get US fund portfolio for {fund_code}: {e}")
        
        return pd.DataFrame()
    
    async def get_fund_holdings_change(
        self, 
        fund_code: str,
        market: str = 'cn',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get fund holdings changes over time.
        
        Args:
            fund_code: Fund code
            market: Market code
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with fund holdings changes
        """
        if market not in self.supported_markets:
            raise FundPortfolioError(f"Unsupported market: {market}")
        
        if market == 'cn':
            return await self._get_cn_fund_holdings_change(fund_code, start_date, end_date)
        elif market == 'hk':
            return await self._get_hk_fund_holdings_change(fund_code, start_date, end_date)
        elif market == 'us':
            return await self._get_us_fund_holdings_change(fund_code, start_date, end_date)
        
        return pd.DataFrame()
    
    async def _get_cn_fund_holdings_change(
        self, 
        fund_code: str, 
        start_date: Optional[str], 
        end_date: Optional[str]
    ) -> pd.DataFrame:
        """Get China fund holdings changes."""
        try:
            await acquire_rate_limit('akshare', 'eastmoney')
            
            # Get fund portfolio holdings for multiple periods
            df = await call_akshare(
                ['fund_portfolio_hold_em'],
                {'fund': fund_code},
                function_name='fund_portfolio_hold_em'
            )
            
            if df.empty:
                return df
            
            # Filter by date range if specified
            if start_date:
                df = df[df['报告期'] >= start_date]
            if end_date:
                df = df[df['报告期'] <= end_date]
            
            # Calculate holdings changes
            df = self._calculate_holdings_changes(df)
            
            # Standardize column names
            df = self._standardize_cn_portfolio_columns(df)
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get China fund holdings change for {fund_code}: {e}")
        
        return pd.DataFrame()
    
    async def _get_hk_fund_holdings_change(
        self, 
        fund_code: str, 
        start_date: Optional[str], 
        end_date: Optional[str]
    ) -> pd.DataFrame:
        """Get Hong Kong fund holdings changes."""
        # Similar to CN but may have different data structure
        return await self._get_cn_fund_holdings_change(fund_code, start_date, end_date)
    
    async def _get_us_fund_holdings_change(
        self, 
        fund_code: str, 
        start_date: Optional[str], 
        end_date: Optional[str]
    ) -> pd.DataFrame:
        """Get US fund holdings changes."""
        # Similar to CN but may have different data structure
        return await self._get_cn_fund_holdings_change(fund_code, start_date, end_date)
    
    async def get_fund_top_holdings(
        self, 
        fund_code: str,
        market: str = 'cn',
        top_n: int = 10
    ) -> pd.DataFrame:
        """
        Get fund top holdings.
        
        Args:
            fund_code: Fund code
            market: Market code
            top_n: Number of top holdings to return
            
        Returns:
            DataFrame with fund top holdings
        """
        try:
            # Get fund portfolio
            df = await self.get_fund_portfolio(fund_code, market)
            
            if df.empty:
                return df
            
            # Sort by percentage and get top N
            if 'percentage' in df.columns:
                df = df.sort_values('percentage', ascending=False).head(top_n)
            elif '占净值比例' in df.columns:
                df = df.sort_values('占净值比例', ascending=False).head(top_n)
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get fund top holdings for {fund_code}: {e}")
        
        return pd.DataFrame()
    
    def _calculate_holdings_changes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate holdings changes between periods."""
        if df.empty or len(df) < 2:
            return df
        
        # Sort by report date
        df = df.sort_values('报告期')
        
        # Group by stock symbol and calculate changes
        changes = []
        for symbol in df['股票代码'].unique():
            symbol_data = df[df['股票代码'] == symbol].copy()
            if len(symbol_data) >= 2:
                # Get latest and previous data
                latest = symbol_data.iloc[-1]
                previous = symbol_data.iloc[-2]
                
                # Calculate changes
                shares_change = latest['持股数'] - previous['持股数'] if '持股数' in latest and '持股数' in previous else 0
                value_change = latest['持仓市值'] - previous['持仓市值'] if '持仓市值' in latest and '持仓市值' in previous else 0
                percentage_change = latest['占净值比例'] - previous['占净值比例'] if '占净值比例' in latest and '占净值比例' in previous else 0
                
                changes.append({
                    'symbol': symbol,
                    'stock_name': latest.get('股票名称', ''),
                    'report_date': latest['报告期'],
                    'shares': latest.get('持股数', 0),
                    'market_value': latest.get('持仓市值', 0),
                    'percentage': latest.get('占净值比例', 0),
                    'shares_change': shares_change,
                    'value_change': value_change,
                    'percentage_change': percentage_change
                })
        
        return pd.DataFrame(changes)
    
    def _standardize_cn_portfolio_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for China fund portfolio."""
        column_mapping = {
            '基金代码': 'fund_code',
            '基金名称': 'fund_name',
            '报告期': 'report_date',
            '股票代码': 'symbol',
            '股票名称': 'stock_name',
            '持股数': 'shares',
            '持仓市值': 'market_value',
            '占净值比例': 'percentage',
            '持股数变化': 'shares_change',
            '持仓市值变化': 'value_change',
            '占净值比例变化': 'percentage_change'
        }
        
        # Rename columns that exist
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        if existing_cols:
            df = df.rename(columns=existing_cols)
        
        return df
    
    def _standardize_hk_portfolio_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for Hong Kong fund portfolio."""
        # Similar to CN but may have different column names
        return self._standardize_cn_portfolio_columns(df)
    
    def _standardize_us_portfolio_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for US fund portfolio."""
        # Similar to CN but may have different column names
        return self._standardize_cn_portfolio_columns(df)


# Convenience function for backward compatibility
async def call_fund_portfolio(
    function_name: str,
    params: Dict[str, Any],
    market: str = 'cn'
) -> Tuple[str, pd.DataFrame]:
    """
    Convenience function to call fund portfolio functions.
    
    Args:
        function_name: Function name to call
        params: Parameters for the function
        market: Market code
        
    Returns:
        Tuple of (function_name, DataFrame)
    """
    adapter = FundPortfolioAdapter()
    
    if function_name == 'fund_portfolio':
        df = await adapter.get_fund_portfolio(
            fund_code=params.get('fund_code'),
            market=params.get('market', market),
            report_date=params.get('report_date')
        )
    elif function_name == 'fund_holdings_change':
        df = await adapter.get_fund_holdings_change(
            fund_code=params.get('fund_code'),
            market=params.get('market', market),
            start_date=params.get('start_date'),
            end_date=params.get('end_date')
        )
    elif function_name == 'fund_top_holdings':
        df = await adapter.get_fund_top_holdings(
            fund_code=params.get('fund_code'),
            market=params.get('market', market),
            top_n=params.get('top_n', 10)
        )
    else:
        raise FundPortfolioError(f"Unknown function: {function_name}")
    
    return function_name, df