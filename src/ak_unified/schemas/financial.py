"""
Data models for financial data including indicators and statements.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class FinancialIndicator(BaseModel):
    """Model for financial indicator data."""
    
    symbol: str = Field(..., description="Stock symbol")
    indicator_name: str = Field(..., description="Name of the financial indicator")
    indicator_value: float = Field(..., description="Value of the indicator")
    unit: Optional[str] = Field(None, description="Unit of measurement")
    report_date: datetime = Field(..., description="Report date")
    period: str = Field(..., description="Period type (annual, quarterly)")
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, us, hk)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FinancialStatement(BaseModel):
    """Model for financial statement data."""
    
    symbol: str = Field(..., description="Stock symbol")
    statement_type: str = Field(..., description="Type of statement (balance_sheet, income_statement, cash_flow)")
    period: str = Field(..., description="Period type (annual, quarterly)")
    report_date: datetime = Field(..., description="Report date")
    data: Dict[str, Any] = Field(..., description="Financial statement data")
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, us, hk)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class BalanceSheet(BaseModel):
    """Model for balance sheet data."""
    
    symbol: str = Field(..., description="Stock symbol")
    report_date: datetime = Field(..., description="Report date")
    period: str = Field(..., description="Period type (annual, quarterly)")
    
    # Assets
    total_assets: Optional[float] = Field(None, description="Total assets")
    current_assets: Optional[float] = Field(None, description="Current assets")
    non_current_assets: Optional[float] = Field(None, description="Non-current assets")
    cash_and_equivalents: Optional[float] = Field(None, description="Cash and cash equivalents")
    short_term_investments: Optional[float] = Field(None, description="Short-term investments")
    accounts_receivable: Optional[float] = Field(None, description="Accounts receivable")
    inventory: Optional[float] = Field(None, description="Inventory")
    property_plant_equipment: Optional[float] = Field(None, description="Property, plant and equipment")
    intangible_assets: Optional[float] = Field(None, description="Intangible assets")
    goodwill: Optional[float] = Field(None, description="Goodwill")
    
    # Liabilities
    total_liabilities: Optional[float] = Field(None, description="Total liabilities")
    current_liabilities: Optional[float] = Field(None, description="Current liabilities")
    non_current_liabilities: Optional[float] = Field(None, description="Non-current liabilities")
    short_term_debt: Optional[float] = Field(None, description="Short-term debt")
    accounts_payable: Optional[float] = Field(None, description="Accounts payable")
    long_term_debt: Optional[float] = Field(None, description="Long-term debt")
    
    # Equity
    total_equity: Optional[float] = Field(None, description="Total equity")
    common_stock: Optional[float] = Field(None, description="Common stock")
    retained_earnings: Optional[float] = Field(None, description="Retained earnings")
    treasury_stock: Optional[float] = Field(None, description="Treasury stock")
    
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, us, hk)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class IncomeStatement(BaseModel):
    """Model for income statement data."""
    
    symbol: str = Field(..., description="Stock symbol")
    report_date: datetime = Field(..., description="Report date")
    period: str = Field(..., description="Period type (annual, quarterly)")
    
    # Revenue
    total_revenue: Optional[float] = Field(None, description="Total revenue")
    net_revenue: Optional[float] = Field(None, description="Net revenue")
    cost_of_revenue: Optional[float] = Field(None, description="Cost of revenue")
    
    # Gross profit
    gross_profit: Optional[float] = Field(None, description="Gross profit")
    gross_margin: Optional[float] = Field(None, description="Gross margin percentage")
    
    # Operating expenses
    operating_expenses: Optional[float] = Field(None, description="Operating expenses")
    selling_general_administrative: Optional[float] = Field(None, description="Selling, general and administrative")
    research_development: Optional[float] = Field(None, description="Research and development")
    
    # Operating income
    operating_income: Optional[float] = Field(None, description="Operating income")
    operating_margin: Optional[float] = Field(None, description="Operating margin percentage")
    
    # Non-operating items
    interest_expense: Optional[float] = Field(None, description="Interest expense")
    other_income_expense: Optional[float] = Field(None, description="Other income/expense")
    
    # Net income
    net_income: Optional[float] = Field(None, description="Net income")
    net_margin: Optional[float] = Field(None, description="Net margin percentage")
    eps: Optional[float] = Field(None, description="Earnings per share")
    diluted_eps: Optional[float] = Field(None, description="Diluted earnings per share")
    
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, us, hk)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class CashFlowStatement(BaseModel):
    """Model for cash flow statement data."""
    
    symbol: str = Field(..., description="Stock symbol")
    report_date: datetime = Field(..., description="Report date")
    period: str = Field(..., description="Period type (annual, quarterly)")
    
    # Operating cash flow
    operating_cash_flow: Optional[float] = Field(None, description="Operating cash flow")
    net_income: Optional[float] = Field(None, description="Net income")
    depreciation_amortization: Optional[float] = Field(None, description="Depreciation and amortization")
    changes_in_working_capital: Optional[float] = Field(None, description="Changes in working capital")
    
    # Investing cash flow
    investing_cash_flow: Optional[float] = Field(None, description="Investing cash flow")
    capital_expenditures: Optional[float] = Field(None, description="Capital expenditures")
    acquisitions: Optional[float] = Field(None, description="Acquisitions")
    investments: Optional[float] = Field(None, description="Investments")
    
    # Financing cash flow
    financing_cash_flow: Optional[float] = Field(None, description="Financing cash flow")
    debt_issuance: Optional[float] = Field(None, description="Debt issuance")
    debt_repayment: Optional[float] = Field(None, description="Debt repayment")
    dividend_payments: Optional[float] = Field(None, description="Dividend payments")
    share_repurchases: Optional[float] = Field(None, description="Share repurchases")
    
    # Net cash flow
    net_cash_flow: Optional[float] = Field(None, description="Net cash flow")
    beginning_cash: Optional[float] = Field(None, description="Beginning cash balance")
    ending_cash: Optional[float] = Field(None, description="Ending cash balance")
    
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, us, hk)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FinancialRatio(BaseModel):
    """Model for financial ratio data."""
    
    symbol: str = Field(..., description="Stock symbol")
    report_date: datetime = Field(..., description="Report date")
    period: str = Field(..., description="Period type (annual, quarterly)")
    
    # Profitability ratios
    roe: Optional[float] = Field(None, description="Return on Equity")
    roa: Optional[float] = Field(None, description="Return on Assets")
    roic: Optional[float] = Field(None, description="Return on Invested Capital")
    gross_margin: Optional[float] = Field(None, description="Gross margin")
    operating_margin: Optional[float] = Field(None, description="Operating margin")
    net_margin: Optional[float] = Field(None, description="Net margin")
    
    # Liquidity ratios
    current_ratio: Optional[float] = Field(None, description="Current ratio")
    quick_ratio: Optional[float] = Field(None, description="Quick ratio")
    cash_ratio: Optional[float] = Field(None, description="Cash ratio")
    
    # Solvency ratios
    debt_to_equity: Optional[float] = Field(None, description="Debt to equity ratio")
    debt_to_assets: Optional[float] = Field(None, description="Debt to assets ratio")
    interest_coverage: Optional[float] = Field(None, description="Interest coverage ratio")
    
    # Efficiency ratios
    asset_turnover: Optional[float] = Field(None, description="Asset turnover ratio")
    inventory_turnover: Optional[float] = Field(None, description="Inventory turnover ratio")
    receivables_turnover: Optional[float] = Field(None, description="Receivables turnover ratio")
    
    # Growth ratios
    revenue_growth: Optional[float] = Field(None, description="Revenue growth rate")
    earnings_growth: Optional[float] = Field(None, description="Earnings growth rate")
    asset_growth: Optional[float] = Field(None, description="Asset growth rate")
    
    source: str = Field(..., description="Data source")
    market: str = Field(..., description="Market code (cn, us, hk)")
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FinancialDataRequest(BaseModel):
    """Request model for financial data."""
    
    symbol: str = Field(..., description="Stock symbol")
    market: str = Field(..., description="Market code (cn, us, hk)")
    period: str = Field(..., description="Period type (annual, quarterly)")
    indicators: Optional[List[str]] = Field(None, description="List of specific indicators to get")
    statement_type: Optional[str] = Field(None, description="Type of financial statement")


class FinancialIndicatorsResponse(BaseModel):
    """Response model for financial indicators."""
    
    success: bool = Field(..., description="Request success status")
    symbol: str = Field(..., description="Stock symbol")
    indicators: List[FinancialIndicator] = Field(..., description="List of financial indicators")
    total_count: int = Field(..., description="Total number of indicators")
    period: str = Field(..., description="Period type")
    market: str = Field(..., description="Market code")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FinancialStatementResponse(BaseModel):
    """Response model for financial statements."""
    
    success: bool = Field(..., description="Request success status")
    symbol: str = Field(..., description="Stock symbol")
    statement_type: str = Field(..., description="Type of financial statement")
    statement: Union[BalanceSheet, IncomeStatement, CashFlowStatement] = Field(..., description="Financial statement data")
    period: str = Field(..., description="Period type")
    market: str = Field(..., description="Market code")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FinancialRatioResponse(BaseModel):
    """Response model for financial ratios."""
    
    success: bool = Field(..., description="Request success status")
    symbol: str = Field(..., description="Stock symbol")
    ratios: List[FinancialRatio] = Field(..., description="List of financial ratios")
    total_count: int = Field(..., description="Total number of ratios")
    period: str = Field(..., description="Period type")
    market: str = Field(..., description="Market code")
    source: str = Field(..., description="Data source")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }