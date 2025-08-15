"""
Data models for fund portfolio data including holdings and changes.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class StockHolding(BaseModel):
    """Model for individual stock holding in a fund portfolio."""
    
    symbol: str = Field(..., description="Stock symbol")
    stock_name: str = Field(..., description="Stock name")
    shares: int = Field(..., description="Number of shares held")
    market_value: float = Field(..., description="Market value of holding")
    percentage: float = Field(..., description="Percentage of fund net asset value")
    change_shares: Optional[int] = Field(None, description="Change in number of shares")
    change_percentage: Optional[float] = Field(None, description="Change in percentage")
    report_date: datetime = Field(..., description="Report date")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FundPortfolio(BaseModel):
    """Model for fund portfolio data."""
    
    fund_code: str = Field(..., description="Fund code")
    fund_name: str = Field(..., description="Fund name")
    report_date: datetime = Field(..., description="Report date")
    total_assets: Optional[float] = Field(None, description="Total fund assets")
    stock_holdings: List[StockHolding] = Field(..., description="List of stock holdings")
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, hk, us)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FundHoldingsChange(BaseModel):
    """Model for fund holdings changes over time."""
    
    fund_code: str = Field(..., description="Fund code")
    fund_name: str = Field(..., description="Fund name")
    symbol: str = Field(..., description="Stock symbol")
    stock_name: str = Field(..., description="Stock name")
    report_date: datetime = Field(..., description="Report date")
    shares: int = Field(..., description="Current number of shares")
    market_value: float = Field(..., description="Current market value")
    percentage: float = Field(..., description="Current percentage of NAV")
    shares_change: int = Field(..., description="Change in number of shares")
    value_change: float = Field(..., description="Change in market value")
    percentage_change: float = Field(..., description="Change in percentage")
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, hk, us)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FundTopHoldings(BaseModel):
    """Model for fund top holdings."""
    
    fund_code: str = Field(..., description="Fund code")
    fund_name: str = Field(..., description="Fund name")
    report_date: datetime = Field(..., description="Report date")
    top_holdings: List[StockHolding] = Field(..., description="List of top holdings")
    total_percentage: float = Field(..., description="Total percentage of top holdings")
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, hk, us)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FundPortfolioRequest(BaseModel):
    """Request model for fund portfolio data."""
    
    fund_code: str = Field(..., description="Fund code")
    market: str = Field(..., description="Market code (cn, hk, us)")
    report_date: Optional[str] = Field(None, description="Report date in YYYY-MM-DD format")


class FundHoldingsChangeRequest(BaseModel):
    """Request model for fund holdings changes."""
    
    fund_code: str = Field(..., description="Fund code")
    market: str = Field(..., description="Market code (cn, hk, us)")
    start_date: Optional[str] = Field(None, description="Start date in YYYY-MM-DD format")
    end_date: Optional[str] = Field(None, description="End date in YYYY-MM-DD format")


class FundTopHoldingsRequest(BaseModel):
    """Request model for fund top holdings."""
    
    fund_code: str = Field(..., description="Fund code")
    market: str = Field(..., description="Market code (cn, hk, us)")
    top_n: int = Field(10, description="Number of top holdings to return")


class FundPortfolioResponse(BaseModel):
    """Response model for fund portfolio."""
    
    success: bool = Field(..., description="Request success status")
    fund_code: str = Field(..., description="Fund code")
    fund_name: str = Field(..., description="Fund name")
    portfolio: Optional[FundPortfolio] = Field(None, description="Fund portfolio data")
    total_holdings: int = Field(..., description="Total number of holdings")
    market: str = Field(..., description="Market code")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FundHoldingsChangeResponse(BaseModel):
    """Response model for fund holdings changes."""
    
    success: bool = Field(..., description="Request success status")
    fund_code: str = Field(..., description="Fund code")
    fund_name: str = Field(..., description="Fund name")
    changes: List[FundHoldingsChange] = Field(..., description="List of holdings changes")
    total_changes: int = Field(..., description="Total number of changes")
    market: str = Field(..., description="Market code")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FundTopHoldingsResponse(BaseModel):
    """Response model for fund top holdings."""
    
    success: bool = Field(..., description="Request success status")
    fund_code: str = Field(..., description="Fund code")
    fund_name: str = Field(..., description="Fund name")
    top_holdings: Optional[FundTopHoldings] = Field(None, description="Fund top holdings data")
    market: str = Field(..., description="Market code")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FundAnalysis(BaseModel):
    """Model for fund portfolio analysis."""
    
    fund_code: str = Field(..., description="Fund code")
    fund_name: str = Field(..., description="Fund name")
    report_date: datetime = Field(..., description="Report date")
    
    # Portfolio composition
    total_stocks: int = Field(..., description="Total number of stocks")
    total_bonds: int = Field(..., description="Total number of bonds")
    total_cash: float = Field(..., description="Total cash position")
    
    # Sector allocation
    sector_allocation: Dict[str, float] = Field(..., description="Sector allocation percentages")
    
    # Top holdings analysis
    top_10_percentage: float = Field(..., description="Percentage of top 10 holdings")
    top_20_percentage: float = Field(..., description="Percentage of top 20 holdings")
    
    # Risk metrics
    concentration_risk: float = Field(..., description="Portfolio concentration risk")
    sector_concentration: float = Field(..., description="Sector concentration risk")
    
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, hk, us)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FundComparison(BaseModel):
    """Model for comparing multiple funds."""
    
    comparison_date: datetime = Field(..., description="Comparison date")
    funds: List[FundAnalysis] = Field(..., description="List of fund analyses")
    metrics: Dict[str, List[float]] = Field(..., description="Comparison metrics")
    rankings: Dict[str, List[str]] = Field(..., description="Fund rankings by metric")
    source: str = Field(..., description="Data source")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }