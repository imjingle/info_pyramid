"""
Data models for Snowball data source including quotes, research reports, and sentiment data.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SnowballQuote(BaseModel):
    """Model for Snowball stock quote data."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, hk, us)")
    name: str = Field(..., description="Stock name")
    current: float = Field(..., description="Current price")
    change: float = Field(..., description="Price change")
    change_percent: float = Field(..., description="Price change percentage")
    open: float = Field(..., description="Open price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    volume: int = Field(..., description="Trading volume")
    market_cap: float = Field(..., description="Market capitalization")
    pe_ratio: float = Field(..., description="P/E ratio")
    pb_ratio: float = Field(..., description="P/B ratio")
    dividend_yield: float = Field(..., description="Dividend yield")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballFinancialData(BaseModel):
    """Model for Snowball financial data."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, hk, us)")
    period: str = Field(..., description="Report period")
    report_date: str = Field(..., description="Report date")
    revenue: float = Field(..., description="Revenue")
    net_profit: float = Field(..., description="Net profit")
    eps: float = Field(..., description="Earnings per share")
    roe: float = Field(..., description="Return on equity")
    roa: float = Field(..., description="Return on assets")
    gross_margin: float = Field(..., description="Gross margin")
    net_margin: float = Field(..., description="Net margin")
    debt_ratio: float = Field(..., description="Debt ratio")
    current_ratio: float = Field(..., description="Current ratio")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballResearchReport(BaseModel):
    """Model for Snowball research report data."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, hk, us)")
    title: str = Field(..., description="Report title")
    author: str = Field(..., description="Report author")
    institution: str = Field(..., description="Institution")
    publish_date: str = Field(..., description="Publish date")
    rating: str = Field(..., description="Rating")
    target_price: float = Field(..., description="Target price")
    summary: str = Field(..., description="Report summary")
    url: str = Field(..., description="Report URL")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballSentiment(BaseModel):
    """Model for Snowball sentiment data."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, hk, us)")
    date: str = Field(..., description="Date")
    positive_count: int = Field(..., description="Positive sentiment count")
    negative_count: int = Field(..., description="Negative sentiment count")
    neutral_count: int = Field(..., description="Neutral sentiment count")
    sentiment_score: float = Field(..., description="Sentiment score")
    discussion_count: int = Field(..., description="Discussion count")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballDiscussion(BaseModel):
    """Model for Snowball discussion data."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, hk, us)")
    title: str = Field(..., description="Discussion title")
    content: str = Field(..., description="Discussion content")
    author: str = Field(..., description="Author")
    publish_time: str = Field(..., description="Publish time")
    like_count: int = Field(..., description="Like count")
    comment_count: int = Field(..., description="Comment count")
    share_count: int = Field(..., description="Share count")
    sentiment: str = Field(..., description="Sentiment")
    url: str = Field(..., description="Discussion URL")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballMarketOverview(BaseModel):
    """Model for Snowball market overview data."""
    
    market: str = Field(..., description="Market code (cn, hk, us)")
    index_name: str = Field(..., description="Index name")
    current_value: float = Field(..., description="Current index value")
    change: float = Field(..., description="Index change")
    change_percent: float = Field(..., description="Index change percentage")
    volume: int = Field(..., description="Trading volume")
    turnover: float = Field(..., description="Turnover")
    advance_count: int = Field(..., description="Advancing stock count")
    decline_count: int = Field(..., description="Declining stock count")
    flat_count: int = Field(..., description="Flat stock count")
    timestamp: datetime = Field(default_factory=datetime.now, description="Data timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballQuoteRequest(BaseModel):
    """Request model for Snowball stock quote."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, hk, us)")


class SnowballFinancialDataRequest(BaseModel):
    """Request model for Snowball financial data."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, hk, us)")
    period: str = Field(..., description="Period type (annual, quarterly)")


class SnowballResearchReportRequest(BaseModel):
    """Request model for Snowball research reports."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, hk, us)")
    limit: int = Field(20, description="Number of reports to return")


class SnowballSentimentRequest(BaseModel):
    """Request model for Snowball sentiment data."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, hk, us)")
    days: int = Field(7, description="Number of days to analyze")


class SnowballDiscussionRequest(BaseModel):
    """Request model for Snowball discussions."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, hk, us)")
    limit: int = Field(50, description="Number of discussions to return")


class SnowballMarketOverviewRequest(BaseModel):
    """Request model for Snowball market overview."""
    
    market: str = Field(..., description="Market code (cn, hk, us)")


class SnowballQuoteResponse(BaseModel):
    """Response model for Snowball stock quote."""
    
    success: bool = Field(..., description="Request success status")
    symbol: str = Field(..., description="Stock symbol")
    quote: SnowballQuote = Field(..., description="Stock quote data")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballFinancialDataResponse(BaseModel):
    """Response model for Snowball financial data."""
    
    success: bool = Field(..., description="Request success status")
    symbol: str = Field(..., description="Stock symbol")
    financial_data: List[SnowballFinancialData] = Field(..., description="Financial data list")
    total_count: int = Field(..., description="Total number of records")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballResearchReportResponse(BaseModel):
    """Response model for Snowball research reports."""
    
    success: bool = Field(..., description="Request success status")
    symbol: str = Field(..., description="Stock symbol")
    reports: List[SnowballResearchReport] = Field(..., description="Research report list")
    total_count: int = Field(..., description="Total number of reports")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballSentimentResponse(BaseModel):
    """Response model for Snowball sentiment data."""
    
    success: bool = Field(..., description="Request success status")
    symbol: str = Field(..., description="Stock symbol")
    sentiment_data: List[SnowballSentiment] = Field(..., description="Sentiment data list")
    total_count: int = Field(..., description="Total number of records")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballDiscussionResponse(BaseModel):
    """Response model for Snowball discussions."""
    
    success: bool = Field(..., description="Request success status")
    symbol: str = Field(..., description="Stock symbol")
    discussions: List[SnowballDiscussion] = Field(..., description="Discussion list")
    total_count: int = Field(..., description="Total number of discussions")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SnowballMarketOverviewResponse(BaseModel):
    """Response model for Snowball market overview."""
    
    success: bool = Field(..., description="Request success status")
    market: str = Field(..., description="Market code")
    overview: SnowballMarketOverview = Field(..., description="Market overview data")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }