from __future__ import annotations

from typing import Dict, Optional
from pydantic import BaseModel, Field


class MacroIndicator(BaseModel):
    region: str
    indicator_id: str
    indicator_name: str
    date: str
    value: float
    unit: Optional[str] = None
    source: Optional[str] = None
    release_time: Optional[str] = None
    period: Optional[str] = Field(default=None, description="M|Q|Y")
    revised: Optional[bool] = None


class MarketQuote(BaseModel):
    symbol: str
    symbol_name: Optional[str] = None
    datetime: str
    last: float
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    prev_close: Optional[float] = None
    change: Optional[float] = None
    pct_change: Optional[float] = None
    volume: Optional[float] = None
    amount: Optional[float] = None
    turnover_rate: Optional[float] = None
    bid1: Optional[float] = None
    ask1: Optional[float] = None


class OHLCVBar(BaseModel):
    symbol: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: Optional[float] = None
    adjust: Optional[str] = Field(default=None, description="none|qfq|hfq")


class IndexConstituent(BaseModel):
    index_symbol: str
    symbol: str
    symbol_name: Optional[str] = None
    weight: Optional[float] = None
    date: Optional[str] = None


class CapitalFlow(BaseModel):
    symbol: str
    date: str
    main_inflow: Optional[float] = None
    main_outflow: Optional[float] = None
    net_inflow: Optional[float] = None
    pct_main: Optional[float] = None


class TradingCalendar(BaseModel):
    date: str
    is_trading_day: bool
    market: str
    open_time: Optional[str] = None
    close_time: Optional[str] = None


class CorporateAction(BaseModel):
    symbol: str
    action_type: str
    ex_date: str
    record_date: Optional[str] = None
    payable_date: Optional[str] = None
    cash_dividend: Optional[float] = None
    stock_dividend_ratio: Optional[float] = None
    split_ratio: Optional[float] = None


class FinancialStatement(BaseModel):
    symbol: str
    statement_type: str
    period_end: str
    report_type: Optional[str] = None
    currency: str
    values: Dict[str, float] = Field(default_factory=dict)


class FundNAV(BaseModel):
    fund_code: str
    fund_name: Optional[str] = None
    nav_date: str
    nav: float
    acc_nav: Optional[float] = None
    daily_return: Optional[float] = None
    subscription_status: Optional[str] = None
    redemption_status: Optional[str] = None
    fee: Optional[float] = None


class BondQuote(BaseModel):
    symbol: str
    date: str
    yield_: Optional[float] = Field(default=None, alias="yield")
    duration: Optional[float] = None
    ytm: Optional[float] = None
    clean_price: Optional[float] = None
    dirty_price: Optional[float] = None
    coupon: Optional[float] = None
    maturity_date: Optional[str] = None


class BondCurve(BaseModel):
    curve_id: str
    date: str
    tenor: str
    yield_: float = Field(alias="yield")


class FuturesContract(BaseModel):
    contract: str
    exchange: Optional[str] = None
    underlying: Optional[str] = None
    delivery_month: Optional[str] = None
    last_trade_date: Optional[str] = None


class FuturesQuote(BaseModel):
    contract: str
    date: str
    open: float
    high: float
    low: float
    close: float
    settlement: Optional[float] = None
    volume: float
    open_interest: Optional[float] = None
    basis: Optional[float] = None


class OptionContract(BaseModel):
    contract: str
    underlying: str
    type: str
    strike: float
    expiry: str


class OptionQuote(BaseModel):
    contract: str
    datetime: str
    last: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[float] = None
    open_interest: Optional[float] = None
    iv: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    vega: Optional[float] = None
    theta: Optional[float] = None
    rho: Optional[float] = None