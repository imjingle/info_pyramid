"""
Financial Data Adapter

Provides access to financial data including:
- Financial indicators (ROE, ROA, etc.)
- Financial statements (Balance Sheet, Income Statement, Cash Flow)
- Financial ratios and metrics
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


class FinancialDataError(BaseAdapterError):
    """Financial data specific error."""
    pass


class FinancialDataAdapter:
    """Adapter for financial data from multiple sources."""
    
    def __init__(self):
        self.supported_markets = ['cn', 'us', 'hk']
        self.supported_statements = ['balance_sheet', 'income_statement', 'cash_flow']
        self.supported_periods = ['annual', 'quarterly']
    
    async def get_financial_indicators(
        self, 
        symbol: str, 
        market: str = 'cn',
        period: str = 'annual',
        indicators: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Get financial indicators for a specific symbol.
        
        Args:
            symbol: Stock symbol
            market: Market code ('cn' for China, 'us' for US, 'hk' for Hong Kong)
            period: Period type ('annual' or 'quarterly')
            indicators: List of specific indicators to get (None for all)
            
        Returns:
            DataFrame with financial indicators
        """
        if market not in self.supported_markets:
            raise FinancialDataError(f"Unsupported market: {market}")
        
        if period not in self.supported_periods:
            raise FinancialDataError(f"Unsupported period: {period}")
        
        if market == 'cn':
            return await self._get_cn_financial_indicators(symbol, period, indicators)
        elif market == 'us':
            return await self._get_us_financial_indicators(symbol, period, indicators)
        elif market == 'hk':
            return await self._get_hk_financial_indicators(symbol, period, indicators)
        
        return pd.DataFrame()
    
    async def _get_cn_financial_indicators(
        self, 
        symbol: str, 
        period: str, 
        indicators: Optional[List[str]]
    ) -> pd.DataFrame:
        """Get financial indicators for China A-share."""
        try:
            await acquire_rate_limit('akshare', 'eastmoney')
            
            # Get comprehensive financial analysis indicators
            df = await call_akshare(
                ['stock_financial_analysis_indicator'],
                {'symbol': symbol},
                function_name='stock_financial_analysis_indicator'
            )
            
            if df.empty:
                return df
            
            # Filter by period
            if period == 'quarterly':
                df = df[df['report_date'].str.contains('Q', na=False)]
            else:  # annual
                df = df[~df['report_date'].str.contains('Q', na=False)]
            
            # Filter by specific indicators if requested
            if indicators:
                available_indicators = [col for col in df.columns if col not in ['symbol', 'report_date']]
                selected_indicators = [ind for ind in indicators if ind in available_indicators]
                if selected_indicators:
                    df = df[['symbol', 'report_date'] + selected_indicators]
            
            # Standardize column names
            df = self._standardize_cn_indicator_columns(df)
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get EastMoney financial indicators for {symbol}: {e}")
            
            # Fallback to Sina
            try:
                await acquire_rate_limit('akshare', 'sina')
                df = await call_akshare(
                    ['stock_financial_analysis_indicator_sina'],
                    {'symbol': symbol},
                    function_name='stock_financial_analysis_indicator_sina'
                )
                
                if not df.empty:
                    df = self._standardize_cn_indicator_columns(df)
                    return df
                    
            except Exception as e2:
                logger.warning(f"Failed to get Sina financial indicators for {symbol}: {e2}")
        
        return pd.DataFrame()
    
    async def _get_us_financial_indicators(
        self, 
        symbol: str, 
        period: str, 
        indicators: Optional[List[str]]
    ) -> pd.DataFrame:
        """Get financial indicators for US stock."""
        try:
            await acquire_rate_limit('alphavantage', 'default')
            
            # Get income statement for key metrics
            income_df = await call_alphavantage(
                'INCOME_STATEMENT',
                {'symbol': symbol}
            )
            
            if not income_df.empty:
                # Calculate key financial indicators
                df = self._calculate_us_financial_indicators(income_df, period)
                
                # Filter by specific indicators if requested
                if indicators:
                    available_indicators = [col for col in df.columns if col not in ['symbol', 'fiscalDateEnding']]
                    selected_indicators = [ind for ind in indicators if ind in available_indicators]
                    if selected_indicators:
                        df = df[['symbol', 'fiscalDateEnding'] + selected_indicators]
                
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get Alpha Vantage financial indicators for {symbol}: {e}")
        
        # Fallback to Yahoo Finance
        try:
            await acquire_rate_limit('yfinance', 'default')
            
            # Get financial data from Yahoo Finance
            ticker_data = await call_yfinance(
                ['financials', 'balance_sheet'],
                {'symbol': symbol},
                function_name='financials'
            )
            
            if not ticker_data.empty:
                df = self._calculate_yahoo_financial_indicators(ticker_data, period)
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get Yahoo Finance financial indicators for {symbol}: {e}")
        
        return pd.DataFrame()
    
    async def _get_hk_financial_indicators(
        self, 
        symbol: str, 
        period: str, 
        indicators: Optional[List[str]]
    ) -> pd.DataFrame:
        """Get financial indicators for Hong Kong stock."""
        # Similar to CN but may have different data structure
        return await self._get_cn_financial_indicators(symbol, period, indicators)
    
    async def get_financial_statements(
        self, 
        symbol: str, 
        statement_type: str,
        market: str = 'cn',
        period: str = 'annual'
    ) -> pd.DataFrame:
        """
        Get financial statements for a specific symbol.
        
        Args:
            symbol: Stock symbol
            statement_type: Type of statement ('balance_sheet', 'income_statement', 'cash_flow')
            market: Market code
            period: Period type ('annual' or 'quarterly')
            
        Returns:
            DataFrame with financial statement data
        """
        if market not in self.supported_markets:
            raise FinancialDataError(f"Unsupported market: {market}")
        
        if statement_type not in self.supported_statements:
            raise FinancialDataError(f"Unsupported statement type: {statement_type}")
        
        if period not in self.supported_periods:
            raise FinancialDataError(f"Unsupported period: {period}")
        
        if market == 'cn':
            return await self._get_cn_financial_statements(symbol, statement_type, period)
        elif market == 'us':
            return await self._get_us_financial_statements(symbol, statement_type, period)
        elif market == 'hk':
            return await self._get_hk_financial_statements(symbol, statement_type, period)
        
        return pd.DataFrame()
    
    async def _get_cn_financial_statements(
        self, 
        symbol: str, 
        statement_type: str, 
        period: str
    ) -> pd.DataFrame:
        """Get financial statements for China A-share."""
        try:
            await acquire_rate_limit('akshare', 'eastmoney')
            
            # Map statement type to AkShare function
            function_mapping = {
                'balance_sheet': 'stock_balance_sheet_by_report_em',
                'income_statement': 'stock_profit_sheet_by_report_em',
                'cash_flow': 'stock_cash_flow_sheet_by_report_em'
            }
            
            function_name = function_mapping.get(statement_type)
            if not function_name:
                raise FinancialDataError(f"Unsupported statement type: {statement_type}")
            
            df = await call_akshare(
                [function_name],
                {'symbol': symbol},
                function_name=function_name
            )
            
            if not df.empty:
                # Filter by period
                if period == 'quarterly':
                    df = df[df['REPORT_DATE'].str.contains('Q', na=False)]
                else:  # annual
                    df = df[~df['REPORT_DATE'].str.contains('Q', na=False)]
                
                # Standardize column names
                df = self._standardize_cn_statement_columns(df, statement_type)
                
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get CN {statement_type} for {symbol}: {e}")
        
        return pd.DataFrame()
    
    async def _get_us_financial_statements(
        self, 
        symbol: str, 
        statement_type: str, 
        period: str
    ) -> pd.DataFrame:
        """Get financial statements for US stock."""
        try:
            await acquire_rate_limit('alphavantage', 'default')
            
            # Map statement type to Alpha Vantage function
            function_mapping = {
                'balance_sheet': 'BALANCE_SHEET',
                'income_statement': 'INCOME_STATEMENT',
                'cash_flow': 'CASH_FLOW'
            }
            
            function_name = function_mapping.get(statement_type)
            if not function_name:
                raise FinancialDataError(f"Unsupported statement type: {statement_type}")
            
            df = await call_alphavantage(
                function_name,
                {'symbol': symbol}
            )
            
            if not df.empty:
                # Filter by period
                if period == 'quarterly':
                    df = df[df['fiscalPeriod'].str.contains('Q', na=False)]
                else:  # annual
                    df = df[~df['fiscalPeriod'].str.contains('Q', na=False)]
                
                # Standardize column names
                df = self._standardize_us_statement_columns(df, statement_type)
                
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get Alpha Vantage {statement_type} for {symbol}: {e}")
        
        # Fallback to Yahoo Finance
        try:
            await acquire_rate_limit('yfinance', 'default')
            
            # Map statement type to Yahoo Finance function
            function_mapping = {
                'balance_sheet': 'balance_sheet',
                'income_statement': 'financials',
                'cash_flow': 'cashflow'
            }
            
            function_name = function_mapping.get(statement_type)
            if not function_name:
                raise FinancialDataError(f"Unsupported statement type: {statement_type}")
            
            df = await call_yfinance(
                [function_name],
                {'symbol': symbol},
                function_name=function_name
            )
            
            if not df.empty:
                df = self._standardize_yahoo_statement_columns(df, statement_type)
                return df
                
        except Exception as e:
            logger.warning(f"Failed to get Yahoo Finance {statement_type} for {symbol}: {e}")
        
        return pd.DataFrame()
    
    async def _get_hk_financial_statements(
        self, 
        symbol: str, 
        statement_type: str, 
        period: str
    ) -> pd.DataFrame:
        """Get financial statements for Hong Kong stock."""
        # Similar to CN but may have different data structure
        return await self._get_cn_financial_statements(symbol, statement_type, period)
    
    def _calculate_us_financial_indicators(self, income_df: pd.DataFrame, period: str) -> pd.DataFrame:
        """Calculate financial indicators from US income statement."""
        indicators = []
        
        for _, row in income_df.iterrows():
            try:
                total_revenue = float(row.get('totalRevenue', 0) or 0)
                gross_profit = float(row.get('grossProfit', 0) or 0)
                net_income = float(row.get('netIncome', 0) or 0)
                total_assets = float(row.get('totalAssets', 0) or 0)
                
                # Calculate key ratios
                gross_margin = (gross_profit / total_revenue * 100) if total_revenue > 0 else 0
                net_margin = (net_income / total_revenue * 100) if total_revenue > 0 else 0
                roa = (net_income / total_assets * 100) if total_assets > 0 else 0
                
                indicators.append({
                    'symbol': row.get('symbol', ''),
                    'fiscalDateEnding': row.get('fiscalDateEnding', ''),
                    'total_revenue': total_revenue,
                    'gross_profit': gross_profit,
                    'net_income': net_income,
                    'gross_margin': gross_margin,
                    'net_margin': net_margin,
                    'roa': roa
                })
                
            except (ValueError, TypeError):
                continue
        
        return pd.DataFrame(indicators)
    
    def _calculate_yahoo_financial_indicators(self, ticker_data: pd.DataFrame, period: str) -> pd.DataFrame:
        """Calculate financial indicators from Yahoo Finance data."""
        # This is a placeholder - actual implementation would depend on Yahoo Finance data structure
        return pd.DataFrame()
    
    def _standardize_cn_indicator_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names for China financial indicators."""
        column_mapping = {
            'symbol': 'symbol',
            'report_date': 'report_date',
            'ROE': 'roe',
            'ROA': 'roa',
            'ROIC': 'roic',
            'gross_profit_margin': 'gross_margin',
            'net_profit_margin': 'net_margin',
            'debt_to_equity': 'debt_to_equity',
            'current_ratio': 'current_ratio',
            'quick_ratio': 'quick_ratio',
            'inventory_turnover': 'inventory_turnover',
            'receivables_turnover': 'receivables_turnover',
            'asset_turnover': 'asset_turnover'
        }
        
        # Rename columns that exist
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        if existing_cols:
            df = df.rename(columns=existing_cols)
        
        return df
    
    def _standardize_cn_statement_columns(self, df: pd.DataFrame, statement_type: str) -> pd.DataFrame:
        """Standardize column names for China financial statements."""
        if statement_type == 'balance_sheet':
            column_mapping = {
                'REPORT_DATE': 'report_date',
                'TOTAL_ASSETS': 'total_assets',
                'TOTAL_LIABILITIES': 'total_liabilities',
                'TOTAL_EQUITY': 'total_equity',
                'CURRENT_ASSETS': 'current_assets',
                'CURRENT_LIABILITIES': 'current_liabilities',
                'CASH_AND_EQUIVALENTS': 'cash_and_equivalents'
            }
        elif statement_type == 'income_statement':
            column_mapping = {
                'REPORT_DATE': 'report_date',
                'TOTAL_REVENUE': 'total_revenue',
                'GROSS_PROFIT': 'gross_profit',
                'OPERATING_INCOME': 'operating_income',
                'NET_INCOME': 'net_income',
                'EPS': 'eps'
            }
        elif statement_type == 'cash_flow':
            column_mapping = {
                'REPORT_DATE': 'report_date',
                'OPERATING_CASH_FLOW': 'operating_cash_flow',
                'INVESTING_CASH_FLOW': 'investing_cash_flow',
                'FINANCING_CASH_FLOW': 'financing_cash_flow',
                'NET_CASH_FLOW': 'net_cash_flow'
            }
        else:
            return df
        
        # Rename columns that exist
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        if existing_cols:
            df = df.rename(columns=existing_cols)
        
        return df
    
    def _standardize_us_statement_columns(self, df: pd.DataFrame, statement_type: str) -> pd.DataFrame:
        """Standardize column names for US financial statements."""
        if statement_type == 'balance_sheet':
            column_mapping = {
                'fiscalDateEnding': 'report_date',
                'fiscalPeriod': 'report_period',
                'totalAssets': 'total_assets',
                'totalLiabilities': 'total_liabilities',
                'totalShareholderEquity': 'total_equity',
                'currentAssets': 'current_assets',
                'currentLiabilities': 'current_liabilities',
                'cashAndCashEquivalentsAtCarryingValue': 'cash_and_equivalents'
            }
        elif statement_type == 'income_statement':
            column_mapping = {
                'fiscalDateEnding': 'report_date',
                'fiscalPeriod': 'report_period',
                'totalRevenue': 'total_revenue',
                'grossProfit': 'gross_profit',
                'operatingIncome': 'operating_income',
                'netIncome': 'net_income',
                'ebitda': 'ebitda'
            }
        elif statement_type == 'cash_flow':
            column_mapping = {
                'fiscalDateEnding': 'report_date',
                'fiscalPeriod': 'report_period',
                'operatingCashflow': 'operating_cash_flow',
                'cashflowFromInvestment': 'investing_cash_flow',
                'cashflowFromFinancing': 'financing_cash_flow',
                'netIncome': 'net_income'
            }
        else:
            return df
        
        # Rename columns that exist
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        if existing_cols:
            df = df.rename(columns=existing_cols)
        
        return df
    
    def _standardize_yahoo_statement_columns(self, df: pd.DataFrame, statement_type: str) -> pd.DataFrame:
        """Standardize column names for Yahoo Finance statements."""
        # This is a placeholder - actual implementation would depend on Yahoo Finance data structure
        return df


# Convenience function for backward compatibility
async def call_financial_data(
    function_name: str,
    params: Dict[str, Any],
    market: str = 'cn'
) -> Tuple[str, pd.DataFrame]:
    """
    Convenience function to call financial data functions.
    
    Args:
        function_name: Function name to call
        params: Parameters for the function
        market: Market code
        
    Returns:
        Tuple of (function_name, DataFrame)
    """
    adapter = FinancialDataAdapter()
    
    if function_name == 'financial_indicators':
        df = await adapter.get_financial_indicators(
            symbol=params.get('symbol'),
            market=params.get('market', market),
            period=params.get('period', 'annual'),
            indicators=params.get('indicators')
        )
    elif function_name == 'financial_statements':
        df = await adapter.get_financial_statements(
            symbol=params.get('symbol'),
            statement_type=params.get('statement_type'),
            market=params.get('market', market),
            period=params.get('period', 'annual')
        )
    else:
        raise FinancialDataError(f"Unknown function: {function_name}")
    
    return function_name, df