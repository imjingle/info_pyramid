"""
EasyTrader Adapter

Provides access to EasyTrader data including:
- Account information and balances
- Portfolio holdings and positions
- Trading records and history
- Market data and quotes
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .base import BaseAdapterError
from ..rate_limiter import acquire_rate_limit
from ..logging import logger


class EasyTraderAdapterError(BaseAdapterError):
    """EasyTrader adapter specific error."""
    pass


class EasyTraderAdapter:
    """Adapter for EasyTrader data from easytrader library."""
    
    def __init__(self, broker: str = 'universal'):
        self.supported_brokers = ['universal', 'ht', 'yh', 'gf', 'xq', 'ths']
        self.broker = broker
        self._trader = None
        
        if broker not in self.supported_brokers:
            raise EasyTraderAdapterError(f"Unsupported broker: {broker}")
    
    def _import_easytrader(self):
        """Import easytrader library."""
        try:
            import easytrader
            return easytrader
        except ImportError:
            raise EasyTraderAdapterError("Failed to import easytrader. Please install: pip install easytrader")
    
    def _get_trader(self):
        """Get trader instance."""
        if self._trader is None:
            easytrader = self._import_easytrader()
            
            if self.broker == 'universal':
                self._trader = easytrader.use('universal')
            elif self.broker == 'ht':
                self._trader = easytrader.use('ht')
            elif self.broker == 'yh':
                self._trader = easytrader.use('yh')
            elif self.broker == 'gf':
                self._trader = easytrader.use('gf')
            elif self.broker == 'xq':
                self._trader = easytrader.use('xq')
            elif self.broker == 'ths':
                self._trader = easytrader.use('ths')
        
        return self._trader
    
    async def login(
        self, 
        username: str, 
        password: str, 
        exe_path: Optional[str] = None,
        comm_password: Optional[str] = None
    ) -> bool:
        """
        Login to trading account.
        
        Args:
            username: Username
            password: Password
            exe_path: Executable path for some brokers
            comm_password: Communication password
            
        Returns:
            Login success status
        """
        try:
            await acquire_rate_limit('easytrader', 'login')
            
            trader = self._get_trader()
            
            # Login with broker-specific parameters
            if self.broker in ['ht', 'yh', 'gf']:
                if exe_path:
                    trader.prepare(user=username, password=password, exe_path=exe_path)
                else:
                    trader.prepare(user=username, password=password)
            elif self.broker == 'xq':
                if comm_password:
                    trader.prepare(user=username, password=password, comm_password=comm_password)
                else:
                    trader.prepare(user=username, password=password)
            else:
                trader.prepare(user=username, password=password)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to login to {self.broker}: {e}")
            return False
    
    async def get_account_info(self) -> pd.DataFrame:
        """
        Get account information.
        
        Returns:
            DataFrame with account information
        """
        try:
            await acquire_rate_limit('easytrader', 'default')
            
            trader = self._get_trader()
            
            # Get account info
            account_info = trader.balance
            
            if not account_info:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame([{
                'broker': self.broker,
                'total_assets': account_info.get('总资产', 0),
                'available_cash': account_info.get('可用金额', 0),
                'market_value': account_info.get('证券市值', 0),
                'frozen_cash': account_info.get('冻结金额', 0),
                'total_profit_loss': account_info.get('总盈亏', 0),
                'today_profit_loss': account_info.get('当日盈亏', 0),
                'timestamp': datetime.now()
            }])
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get account info from {self.broker}: {e}")
            return pd.DataFrame()
    
    async def get_portfolio(self) -> pd.DataFrame:
        """
        Get portfolio holdings.
        
        Returns:
            DataFrame with portfolio holdings
        """
        try:
            await acquire_rate_limit('easytrader', 'default')
            
            trader = self._get_trader()
            
            # Get portfolio
            portfolio = trader.position
            
            if not portfolio:
                return pd.DataFrame()
            
            # Convert to DataFrame
            rows = []
            for item in portfolio:
                rows.append({
                    'broker': self.broker,
                    'symbol': item.get('证券代码', ''),
                    'name': item.get('证券名称', ''),
                    'shares': item.get('股票余额', 0),
                    'available_shares': item.get('可用余额', 0),
                    'cost_price': item.get('成本价', 0),
                    'current_price': item.get('最新价', 0),
                    'market_value': item.get('证券市值', 0),
                    'profit_loss': item.get('浮动盈亏', 0),
                    'profit_loss_ratio': item.get('盈亏比例', 0),
                    'timestamp': datetime.now()
                })
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.warning(f"Failed to get portfolio from {self.broker}: {e}")
            return pd.DataFrame()
    
    async def get_trading_history(
        self, 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get trading history.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with trading history
        """
        try:
            await acquire_rate_limit('easytrader', 'default')
            
            trader = self._get_trader()
            
            # Get trading history
            if hasattr(trader, 'trades'):
                trades = trader.trades
            elif hasattr(trader, 'history'):
                trades = trader.history
            else:
                return pd.DataFrame()
            
            if not trades:
                return pd.DataFrame()
            
            # Convert to DataFrame
            rows = []
            for item in trades:
                # Filter by date if specified
                trade_date = item.get('成交日期', '')
                if start_date and trade_date < start_date:
                    continue
                if end_date and trade_date > end_date:
                    continue
                
                rows.append({
                    'broker': self.broker,
                    'trade_date': trade_date,
                    'symbol': item.get('证券代码', ''),
                    'name': item.get('证券名称', ''),
                    'trade_type': item.get('交易类型', ''),
                    'shares': item.get('成交数量', 0),
                    'price': item.get('成交价格', 0),
                    'amount': item.get('成交金额', 0),
                    'commission': item.get('手续费', 0),
                    'stamp_duty': item.get('印花税', 0),
                    'timestamp': datetime.now()
                })
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.warning(f"Failed to get trading history from {self.broker}: {e}")
            return pd.DataFrame()
    
    async def get_market_data(
        self, 
        symbols: List[str]
    ) -> pd.DataFrame:
        """
        Get market data for symbols.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            DataFrame with market data
        """
        try:
            await acquire_rate_limit('easytrader', 'default')
            
            trader = self._get_trader()
            
            # Get market data
            if hasattr(trader, 'market_data'):
                market_data = trader.market_data(symbols)
            elif hasattr(trader, 'quotes'):
                market_data = trader.quotes(symbols)
            else:
                return pd.DataFrame()
            
            if not market_data:
                return pd.DataFrame()
            
            # Convert to DataFrame
            rows = []
            for symbol, data in market_data.items():
                rows.append({
                    'broker': self.broker,
                    'symbol': symbol,
                    'name': data.get('name', ''),
                    'current_price': data.get('current', 0),
                    'change': data.get('change', 0),
                    'change_percent': data.get('change_percent', 0),
                    'open': data.get('open', 0),
                    'high': data.get('high', 0),
                    'low': data.get('low', 0),
                    'volume': data.get('volume', 0),
                    'turnover': data.get('turnover', 0),
                    'timestamp': datetime.now()
                })
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.warning(f"Failed to get market data from {self.broker}: {e}")
            return pd.DataFrame()
    
    async def get_fund_info(self) -> pd.DataFrame:
        """
        Get fund information.
        
        Returns:
            DataFrame with fund information
        """
        try:
            await acquire_rate_limit('easytrader', 'default')
            
            trader = self._get_trader()
            
            # Get fund info
            if hasattr(trader, 'fund_info'):
                fund_info = trader.fund_info
            else:
                return pd.DataFrame()
            
            if not fund_info:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame([{
                'broker': self.broker,
                'fund_code': fund_info.get('资金账号', ''),
                'fund_name': fund_info.get('资金账户', ''),
                'branch': fund_info.get('营业部', ''),
                'status': fund_info.get('状态', ''),
                'timestamp': datetime.now()
            }])
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get fund info from {self.broker}: {e}")
            return pd.DataFrame()
    
    async def get_risk_metrics(self) -> pd.DataFrame:
        """
        Get risk metrics.
        
        Returns:
            DataFrame with risk metrics
        """
        try:
            await acquire_rate_limit('easytrader', 'default')
            
            trader = self._get_trader()
            
            # Get risk metrics
            if hasattr(trader, 'risk_metrics'):
                risk_metrics = trader.risk_metrics
            else:
                return pd.DataFrame()
            
            if not risk_metrics:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame([{
                'broker': self.broker,
                'var': risk_metrics.get('VaR', 0),
                'max_drawdown': risk_metrics.get('最大回撤', 0),
                'sharpe_ratio': risk_metrics.get('夏普比率', 0),
                'volatility': risk_metrics.get('波动率', 0),
                'beta': risk_metrics.get('贝塔系数', 0),
                'timestamp': datetime.now()
            }])
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get risk metrics from {self.broker}: {e}")
            return pd.DataFrame()


# Convenience function for backward compatibility
async def call_easytrader(
    function_name: str,
    params: Dict[str, Any],
    broker: str = 'universal'
) -> Tuple[str, pd.DataFrame]:
    """
    Convenience function to call EasyTrader functions.
    
    Args:
        function_name: Function name to call
        params: Parameters for the function
        broker: Broker name
        
    Returns:
        Tuple of (function_name, DataFrame)
    """
    adapter = EasyTraderAdapter(broker=broker)
    
    if function_name == 'login':
        success = await adapter.login(
            username=params.get('username'),
            password=params.get('password'),
            exe_path=params.get('exe_path'),
            comm_password=params.get('comm_password')
        )
        return 'login', pd.DataFrame({'success': [success]})
    
    elif function_name == 'account_info':
        df = await adapter.get_account_info()
        return 'account_info', df
    
    elif function_name == 'portfolio':
        df = await adapter.get_portfolio()
        return 'portfolio', df
    
    elif function_name == 'trading_history':
        df = await adapter.get_trading_history(
            start_date=params.get('start_date'),
            end_date=params.get('end_date')
        )
        return 'trading_history', df
    
    elif function_name == 'market_data':
        df = await adapter.get_market_data(
            symbols=params.get('symbols', [])
        )
        return 'market_data', df
    
    elif function_name == 'fund_info':
        df = await adapter.get_fund_info()
        return 'fund_info', df
    
    elif function_name == 'risk_metrics':
        df = await adapter.get_risk_metrics()
        return 'risk_metrics', df
    
    else:
        raise EasyTraderAdapterError(f"Unknown function: {function_name}")