"""
Data models for earnings events and forecasts.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class EarningsEvent(BaseModel):
    """Model for earnings event data."""
    
    symbol: str = Field(..., description="Stock symbol")
    company_name: Optional[str] = Field(None, description="Company name")
    report_period: str = Field(..., description="Report period (e.g., '2024', '2024Q1')")
    report_type: str = Field(..., description="Report type (annual, semi_annual, quarterly)")
    scheduled_date: Optional[datetime] = Field(None, description="Scheduled report date")
    actual_date: Optional[datetime] = Field(None, description="Actual report date")
    eps_estimate: Optional[float] = Field(None, description="Estimated EPS")
    eps_actual: Optional[float] = Field(None, description="Actual EPS")
    revenue_estimate: Optional[float] = Field(None, description="Estimated revenue")
    revenue_actual: Optional[float] = Field(None, description="Actual revenue")
    net_profit_estimate: Optional[float] = Field(None, description="Estimated net profit")
    net_profit_actual: Optional[float] = Field(None, description="Actual net profit")
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, us, hk)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    updated_at: datetime = Field(default_factory=datetime.now, description="Record update time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EarningsForecast(BaseModel):
    """Model for earnings forecast data."""
    
    symbol: str = Field(..., description="Stock symbol")
    forecast_period: str = Field(..., description="Forecast period")
    forecast_type: str = Field(..., description="Forecast type (pre_increase, pre_decrease, turn_profit, etc.)")
    net_profit_change: Optional[float] = Field(None, description="Net profit change percentage")
    net_profit_change_min: Optional[float] = Field(None, description="Minimum net profit change")
    net_profit_change_max: Optional[float] = Field(None, description="Maximum net profit change")
    change_reason: Optional[str] = Field(None, description="Reason for change")
    announcement_date: datetime = Field(..., description="Announcement date")
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, us, hk)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ImportantAnnouncement(BaseModel):
    """Model for important announcements."""
    
    symbol: str = Field(..., description="Stock symbol")
    announcement_type: str = Field(..., description="Type of announcement")
    title: str = Field(..., description="Announcement title")
    content: Optional[str] = Field(None, description="Announcement content")
    announcement_date: datetime = Field(..., description="Announcement date")
    effective_date: Optional[datetime] = Field(None, description="Effective date")
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, us, hk)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EarningsCalendarRequest(BaseModel):
    """Request model for earnings calendar."""
    
    market: str = Field(..., description="Market code (cn, us, hk)")
    start_date: Optional[str] = Field(None, description="Start date in YYYY-MM-DD format")
    end_date: Optional[str] = Field(None, description="End date in YYYY-MM-DD format")
    symbols: Optional[List[str]] = Field(None, description="List of stock symbols to filter")
    report_types: Optional[List[str]] = Field(None, description="List of report types to filter")


class EarningsForecastRequest(BaseModel):
    """Request model for earnings forecast."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, us, hk)")
    period: Optional[str] = Field(None, description="Forecast period")


class EarningsCalendarResponse(BaseModel):
    """Response model for earnings calendar."""
    
    success: bool = Field(..., description="Request success status")
    data: List[EarningsEvent] = Field(..., description="List of earnings events")
    total_count: int = Field(..., description="Total number of events")
    market: str = Field(..., description="Market code")
    period: str = Field(..., description="Time period covered")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EarningsForecastResponse(BaseModel):
    """Response model for earnings forecast."""
    
    success: bool = Field(..., description="Request success status")
    symbol: str = Field(..., description="Stock symbol")
    forecasts: List[EarningsForecast] = Field(..., description="List of earnings forecasts")
    total_count: int = Field(..., description="Total number of forecasts")
    market: str = Field(..., description="Market code")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EarningsReminder(BaseModel):
    """Model for earnings reminders."""
    
    symbol: str = Field(..., description="Stock symbol")
    event_type: str = Field(..., description="Type of event (earnings, forecast, announcement)")
    event_date: datetime = Field(..., description="Event date")
    reminder_days: int = Field(..., description="Days before event to send reminder")
    user_id: Optional[str] = Field(None, description="User ID for personalized reminders")
    created_at: datetime = Field(default_factory=datetime.now, description="Reminder creation time")
    is_active: bool = Field(True, description="Whether reminder is active")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }