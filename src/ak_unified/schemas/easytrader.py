"""
Data models for EasyTrader data source including account info, portfolio, and trading data.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class EasyTraderAccountInfo(BaseModel):
    """Model for EasyTrader account information."""
    
    broker: str = Field(..., description="Broker name")
    total_assets: float = Field(..., description="Total assets")
    available_cash: float = Field(..., description="Available cash")
    market_value: float = Field(..., description="Market value")
    frozen_cash: float = Field(..., description="Frozen cash")
    total_profit_loss: float = Field(..., description="Total profit/loss")
    today_profit_loss: float = Field(..., description="Today's profit/loss")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderPosition(BaseModel):
    """Model for EasyTrader position data."""
    
    broker: str = Field(..., description="Broker name")
    symbol: str = Field(..., description="Stock symbol")
    name: str = Field(..., description="Stock name")
    shares: int = Field(..., description="Total shares")
    available_shares: int = Field(..., description="Available shares")
    cost_price: float = Field(..., description="Cost price")
    current_price: float = Field(..., description="Current price")
    market_value: float = Field(..., description="Market value")
    profit_loss: float = Field(..., description="Profit/loss")
    profit_loss_ratio: float = Field(..., description="Profit/loss ratio")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderTrade(BaseModel):
    """Model for EasyTrader trade data."""
    
    broker: str = Field(..., description="Broker name")
    trade_date: str = Field(..., description="Trade date")
    symbol: str = Field(..., description="Stock symbol")
    name: str = Field(..., description="Stock name")
    trade_type: str = Field(..., description="Trade type")
    shares: int = Field(..., description="Trade shares")
    price: float = Field(..., description="Trade price")
    amount: float = Field(..., description="Trade amount")
    commission: float = Field(..., description="Commission")
    stamp_duty: float = Field(..., description="Stamp duty")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderMarketData(BaseModel):
    """Model for EasyTrader market data."""
    
    broker: str = Field(..., description="Broker name")
    symbol: str = Field(..., description="Stock symbol")
    name: str = Field(..., description="Stock name")
    current_price: float = Field(..., description="Current price")
    change: float = Field(..., description="Price change")
    change_percent: float = Field(..., description="Price change percentage")
    open: float = Field(..., description="Open price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    volume: int = Field(..., description="Trading volume")
    turnover: float = Field(..., description="Turnover")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderFundInfo(BaseModel):
    """Model for EasyTrader fund information."""
    
    broker: str = Field(..., description="Broker name")
    fund_code: str = Field(..., description="Fund code")
    fund_name: str = Field(..., description="Fund name")
    branch: str = Field(..., description="Branch")
    status: str = Field(..., description="Status")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderRiskMetrics(BaseModel):
    """Model for EasyTrader risk metrics."""
    
    broker: str = Field(..., description="Broker name")
    var: float = Field(..., description="Value at Risk")
    max_drawdown: float = Field(..., description="Maximum drawdown")
    sharpe_ratio: float = Field(..., description="Sharpe ratio")
    volatility: float = Field(..., description="Volatility")
    beta: float = Field(..., description="Beta coefficient")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderLoginRequest(BaseModel):
    """Request model for EasyTrader login."""
    
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")
    broker: str = Field(..., description="Broker name")
    exe_path: Optional[str] = Field(None, description="Executable path")
    comm_password: Optional[str] = Field(None, description="Communication password")


class EasyTraderTradingHistoryRequest(BaseModel):
    """Request model for EasyTrader trading history."""
    
    broker: str = Field(..., description="Broker name")
    start_date: Optional[str] = Field(None, description="Start date in YYYY-MM-DD format")
    end_date: Optional[str] = Field(None, description="End date in YYYY-MM-DD format")


class EasyTraderMarketDataRequest(BaseModel):
    """Request model for EasyTrader market data."""
    
    broker: str = Field(..., description="Broker name")
    symbols: List[str] = Field(..., description="List of stock symbols")


class EasyTraderLoginResponse(BaseModel):
    """Response model for EasyTrader login."""
    
    success: bool = Field(..., description="Login success status")
    broker: str = Field(..., description="Broker name")
    message: str = Field(..., description="Response message")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderAccountInfoResponse(BaseModel):
    """Response model for EasyTrader account information."""
    
    success: bool = Field(..., description="Request success status")
    broker: str = Field(..., description="Broker name")
    account_info: EasyTraderAccountInfo = Field(..., description="Account information")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderPortfolioResponse(BaseModel):
    """Response model for EasyTrader portfolio."""
    
    success: bool = Field(..., description="Request success status")
    broker: str = Field(..., description="Broker name")
    positions: List[EasyTraderPosition] = Field(..., description="Position list")
    total_count: int = Field(..., description="Total number of positions")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderTradingHistoryResponse(BaseModel):
    """Response model for EasyTrader trading history."""
    
    success: bool = Field(..., description="Request success status")
    broker: str = Field(..., description="Broker name")
    trades: List[EasyTraderTrade] = Field(..., description="Trade list")
    total_count: int = Field(..., description="Total number of trades")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderMarketDataResponse(BaseModel):
    """Response model for EasyTrader market data."""
    
    success: bool = Field(..., description="Request success status")
    broker: str = Field(..., description="Broker name")
    market_data: List[EasyTraderMarketData] = Field(..., description="Market data list")
    total_count: int = Field(..., description="Total number of records")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderFundInfoResponse(BaseModel):
    """Response model for EasyTrader fund information."""
    
    success: bool = Field(..., description="Request success status")
    broker: str = Field(..., description="Broker name")
    fund_info: EasyTraderFundInfo = Field(..., description="Fund information")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EasyTraderRiskMetricsResponse(BaseModel):
    """Response model for EasyTrader risk metrics."""
    
    success: bool = Field(..., description="Request success status")
    broker: str = Field(..., description="Broker name")
    risk_metrics: EasyTraderRiskMetrics = Field(..., description="Risk metrics")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }