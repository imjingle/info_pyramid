"""
Comprehensive tests for Alpha Vantage adapter based on official API documentation.
These tests validate the core functionality of Alpha Vantage data fetching through ak-unified.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from ak_unified.adapters.alphavantage_adapter import (
    call_alphavantage, 
    _parse_daily, 
    _parse_intraday, 
    _parse_global_quote,
    _parse_overview,
    _parse_statement,
    _parse_series,
    AVAdapterError
)


class TestAlphaVantageBasicFunctions:
    """Test basic Alpha Vantage functions."""
    
    @pytest.mark.asyncio
    async def test_time_series_daily_adjusted(self):
        """Test TIME_SERIES_DAILY_ADJUSTED function - 日线调整后数据"""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            mock_get.return_value = {
                "Meta Data": {
                    "1. Information": "Daily Prices (Adjusted) and Volumes",
                    "2. Symbol": "AAPL",
                    "3. Last Refreshed": "2024-01-02",
                    "4. Output Size": "Compact",
                    "5. Time Zone": "US/Eastern"
                },
                "Time Series (Daily)": {
                    "2024-01-02": {
                        "1. open": "185.59",
                        "2. high": "186.12",
                        "3. low": "183.62",
                        "4. close": "185.14",
                        "5. adjusted close": "185.14",
                        "6. volume": "52455991",
                        "7. dividend amount": "0.0000",
                        "8. split coefficient": "1.0"
                    },
                    "2024-01-01": {
                        "1. open": "185.64",
                        "2. high": "186.05",
                        "3. low": "183.88",
                        "4. close": "185.85",
                        "5. adjusted close": "185.85",
                        "6. volume": "45678901",
                        "7. dividend amount": "0.0000",
                        "8. split coefficient": "1.0"
                    }
                }
            }
            
            result = await call_alphavantage(
                "securities.equity.us.ohlcv_daily.av",
                {"symbol": "AAPL"}
            )
            
            assert result[0] == "TIME_SERIES_DAILY_ADJUSTED"
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2
            assert "symbol" in result[1].columns
            assert result[1]["symbol"].iloc[0] == "AAPL"
    
    @pytest.mark.asyncio
    async def test_time_series_intraday(self):
        """Test TIME_SERIES_INTRADAY function - 分钟级数据"""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            mock_get.return_value = {
                "Meta Data": {
                    "1. Information": "Intraday (5min) open, high, low, close prices and volume",
                    "2. Symbol": "AAPL",
                    "3. Last Refreshed": "2024-01-02 16:00:00",
                    "4. Interval": "5min",
                    "5. Output Size": "Compact",
                    "6. Time Zone": "US/Eastern"
                },
                "Time Series (5min)": {
                    "2024-01-02 16:00:00": {
                        "1. open": "185.14",
                        "2. high": "185.20",
                        "3. low": "185.10",
                        "4. close": "185.18",
                        "5. volume": "1234567"
                    },
                    "2024-01-02 15:55:00": {
                        "1. open": "185.12",
                        "2. high": "185.15",
                        "3. low": "185.08",
                        "4. close": "185.14",
                        "5. volume": "987654"
                    }
                }
            }
            
            result = await call_alphavantage(
                "securities.equity.us.ohlcv_min.av",
                {"symbol": "AAPL", "freq": "min5"}
            )
            
            assert result[0] == "TIME_SERIES_INTRADAY_5min"
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2
            assert "symbol" in result[1].columns
            assert result[1]["symbol"].iloc[0] == "AAPL"
    
    @pytest.mark.asyncio
    async def test_global_quote(self):
        """Test GLOBAL_QUOTE function - 实时行情"""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            mock_get.return_value = {
                "Global Quote": {
                    "01. symbol": "AAPL",
                    "02. open": "185.59",
                    "03. high": "186.12",
                    "04. low": "183.62",
                    "05. price": "185.14",
                    "06. volume": "52455991",
                    "07. latest trading day": "2024-01-02",
                    "08. previous close": "185.85",
                    "09. change": "-0.71",
                    "10. change percent": "-0.3821%"
                }
            }
            
            result = await call_alphavantage(
                "securities.equity.us.quote.av",
                {"symbol": "AAPL"}
            )
            
            assert result[0] == "GLOBAL_QUOTE"
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 1
            assert "symbol" in result[1].columns
            assert result[1]["symbol"].iloc[0] == "AAPL"


class TestAlphaVantageFundamentals:
    """Test Alpha Vantage fundamental data functions."""
    
    @pytest.mark.asyncio
    async def test_company_overview(self):
        """Test OVERVIEW function - 公司概览"""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            mock_get.return_value = {
                "Symbol": "AAPL",
                "AssetType": "Common Stock",
                "Name": "Apple Inc",
                "Description": "Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables and accessories, and sells a variety of related services.",
                "CIK": "320193",
                "Exchange": "NASDAQ",
                "Currency": "USD",
                "Country": "USA",
                "Sector": "Technology",
                "Industry": "Consumer Electronics",
                "Address": "One Apple Park Way, Cupertino, CA, 95014",
                "FiscalYearEnd": "September",
                "LatestQuarter": "2023-09-30",
                "MarketCapitalization": "3000000000000",
                "EBITDA": "120000000000",
                "PERatio": "25.5",
                "PEGRatio": "2.1",
                "BookValue": "4.25",
                "DividendPerShare": "0.92",
                "DividendYield": "0.005",
                "EPS": "6.16",
                "RevenuePerShareTTM": "24.5",
                "ProfitMargin": "0.25",
                "OperatingMarginTTM": "0.30",
                "ReturnOnAssetsTTM": "0.20",
                "ReturnOnEquityTTM": "1.50",
                "RevenueTTM": "394328000000",
                "GrossProfitTTM": "170782000000",
                "DilutedEPSTTM": "6.16",
                "QuarterlyEarningsGrowthYOY": "0.13",
                "QuarterlyRevenueGrowthYOY": "0.08",
                "AnalystTargetPrice": "200.00",
                "TrailingPE": "25.5",
                "ForwardPE": "28.0",
                "PriceToBookRatio": "45.0",
                "EVToRevenue": "7.5",
                "EVToEBITDA": "25.0",
                "Beta": "1.3",
                "52WeekHigh": "198.23",
                "52WeekLow": "124.17",
                "50DayMovingAverage": "185.50",
                "200DayMovingAverage": "175.20",
                "SharesOutstanding": "15728700400",
                "DividendDate": "2023-11-16",
                "ExDividendDate": "2023-11-10"
            }
            
            result = await call_alphavantage(
                "securities.equity.us.fundamentals.overview.av",
                {"symbol": "AAPL"}
            )
            
            assert result[0] == "OVERVIEW"
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 1
            assert "symbol" in result[1].columns
            assert result[1]["symbol"].iloc[0] == "AAPL"
    
    @pytest.mark.asyncio
    async def test_income_statement(self):
        """Test INCOME_STATEMENT function - 利润表"""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            mock_get.return_value = {
                "symbol": "AAPL",
                "annualReports": [
                    {
                        "fiscalDateEnding": "2023-09-30",
                        "reportedCurrency": "USD",
                        "grossProfit": "170782000000",
                        "totalRevenue": "394328000000",
                        "costOfRevenue": "223546000000",
                        "costofGoodsAndServicesSold": "223546000000",
                        "operatingIncome": "117669000000",
                        "sellingGeneralAndAdministrative": "25094000000",
                        "researchAndDevelopment": "29915000000",
                        "operatingExpenses": "55009000000",
                        "investmentIncomeNet": "0",
                        "netInterestIncome": "0",
                        "interestIncome": "0",
                        "interestExpense": "0",
                        "nonInterestIncome": "0",
                        "otherNonOperatingIncome": "0",
                        "depreciation": "0",
                        "depreciationAndAmortization": "0",
                        "incomeBeforeTax": "117669000000",
                        "incomeTaxExpense": "16741000000",
                        "interestAndDebtExpense": "0",
                        "netIncomeFromContinuingOperations": "100948000000",
                        "comprehensiveIncomeNetOfTax": "0",
                        "ebit": "0",
                        "ebitda": "0",
                        "netIncome": "100948000000"
                    }
                ],
                "quarterlyReports": [
                    {
                        "fiscalDateEnding": "2023-09-30",
                        "reportedCurrency": "USD",
                        "grossProfit": "45000000000",
                        "totalRevenue": "89498000000",
                        "costOfRevenue": "44498000000",
                        "costofGoodsAndServicesSold": "44498000000",
                        "operatingIncome": "25000000000",
                        "sellingGeneralAndAdministrative": "6000000000",
                        "researchAndDevelopment": "7000000000",
                        "operatingExpenses": "13000000000",
                        "investmentIncomeNet": "0",
                        "netInterestIncome": "0",
                        "interestIncome": "0",
                        "interestExpense": "0",
                        "nonInterestIncome": "0",
                        "otherNonOperatingIncome": "0",
                        "depreciation": "0",
                        "depreciationAndAmortization": "0",
                        "incomeBeforeTax": "25000000000",
                        "incomeTaxExpense": "4000000000",
                        "interestAndDebtExpense": "0",
                        "netIncomeFromContinuingOperations": "21000000000",
                        "comprehensiveIncomeNetOfTax": "0",
                        "ebit": "0",
                        "ebitda": "0",
                        "netIncome": "21000000000"
                    }
                ]
            }
            
            result = await call_alphavantage(
                "securities.equity.us.fundamentals.income_statement.av",
                {"symbol": "AAPL", "period": "annual"}
            )
            
            assert result[0] == "INCOME_STATEMENT"
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 1
            assert "symbol" in result[1].columns
            assert result[1]["symbol"].iloc[0] == "AAPL"
    
    @pytest.mark.asyncio
    async def test_balance_sheet(self):
        """Test BALANCE_SHEET function - 资产负债表"""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            mock_get.return_value = {
                "symbol": "AAPL",
                "annualReports": [
                    {
                        "fiscalDateEnding": "2023-09-30",
                        "reportedCurrency": "USD",
                        "totalAssets": "352755000000",
                        "totalCurrentAssets": "143713000000",
                        "cashAndCashEquivalentsAtCarryingValue": "29965000000",
                        "cashAndShortTermInvestments": "29965000000",
                        "inventory": "6331100000",
                        "currentNetReceivables": "29508000000",
                        "totalNonCurrentAssets": "209042000000",
                        "propertyPlantEquipmentNet": "43667000000",
                        "accumulatedDepreciationAmortizationPPE": "0",
                        "intangibleAssets": "0",
                        "intangibleAssetsExcludingGoodwill": "0",
                        "goodwill": "0",
                        "investments": "0",
                        "longTermInvestments": "0",
                        "shortTermInvestments": "0",
                        "otherCurrentAssets": "0",
                        "otherNonCurrentAssets": "0",
                        "totalLiabilities": "287912000000",
                        "totalCurrentLiabilities": "145308000000",
                        "currentAccountsPayable": "62611000000",
                        "deferredRevenue": "0",
                        "currentDebt": "0",
                        "shortTermDebt": "0",
                        "totalNonCurrentLiabilities": "142604000000",
                        "capitalLeaseObligations": "0",
                        "longTermDebt": "0",
                        "currentLongTermDebt": "0",
                        "longTermDebtNoncurrent": "0",
                        "shortLongTermDebtTotal": "0",
                        "otherCurrentLiabilities": "0",
                        "otherNonCurrentLiabilities": "0",
                        "totalShareholderEquity": "64843000000",
                        "treasuryStock": "0",
                        "retainedEarnings": "0",
                        "commonStock": "0",
                        "commonStockSharesOutstanding": "0"
                    }
                ],
                "quarterlyReports": []
            }
            
            result = await call_alphavantage(
                "securities.equity.us.fundamentals.balance_sheet.av",
                {"symbol": "AAPL", "period": "annual"}
            )
            
            assert result[0] == "BALANCE_SHEET"
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 1
            assert "symbol" in result[1].columns
            assert result[1]["symbol"].iloc[0] == "AAPL"


class TestAlphaVantageMacroData:
    """Test Alpha Vantage macroeconomic data functions."""
    
    @pytest.mark.asyncio
    async def test_cpi_data(self):
        """Test CPI function - 消费者价格指数"""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            mock_get.return_value = {
                "name": "Consumer Price Index",
                "interval": "monthly",
                "unit": "Index 1982-84=100",
                "data": [
                    {
                        "timestamp": "2024-01-31",
                        "value": "308.417"
                    },
                    {
                        "timestamp": "2023-12-31",
                        "value": "307.051"
                    }
                ]
            }
            
            result = await call_alphavantage(
                "macro.us.cpi",
                {}
            )
            
            assert result[0] == "CPI"
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2
    
    @pytest.mark.asyncio
    async def test_pmi_data(self):
        """Test PMI function - 采购经理人指数"""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            mock_get.return_value = {
                "name": "Purchasing Managers' Index",
                "interval": "monthly",
                "unit": "Index",
                "data": [
                    {
                        "timestamp": "2024-01-31",
                        "value": "50.1"
                    },
                    {
                        "timestamp": "2023-12-31",
                        "value": "50.0"
                    }
                ]
            }
            
            result = await call_alphavantage(
                "macro.us.pmi",
                {}
            )
            
            assert result[0] == "PMI"
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2


class TestAlphaVantageDataParsing:
    """Test Alpha Vantage data parsing functions."""
    
    def test_parse_daily(self):
        """Test daily data parsing."""
        data = {
            "Time Series (Daily)": {
                "2024-01-02": {
                    "1. open": "185.59",
                    "2. high": "186.12",
                    "3. low": "183.62",
                    "4. close": "185.14",
                    "5. adjusted close": "185.14",
                    "6. volume": "52455991"
                }
            }
        }
        
        result = _parse_daily(data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "date" in result.columns
        assert "open" in result.columns
        assert "high" in result.columns
        assert "low" in result.columns
        assert "close" in result.columns
        assert "volume" in result.columns
    
    def test_parse_intraday(self):
        """Test intraday data parsing."""
        data = {
            "Time Series (5min)": {
                "2024-01-02 16:00:00": {
                    "1. open": "185.14",
                    "2. high": "185.20",
                    "3. low": "185.10",
                    "4. close": "185.18",
                    "5. volume": "1234567"
                }
            }
        }
        
        result = _parse_intraday(data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "datetime" in result.columns
        assert "open" in result.columns
        assert "high" in result.columns
        assert "low" in result.columns
        assert "close" in result.columns
        assert "volume" in result.columns
    
    def test_parse_global_quote(self):
        """Test global quote parsing."""
        data = {
            "Global Quote": {
                "01. symbol": "AAPL",
                "02. open": "185.59",
                "03. high": "186.12",
                "04. low": "183.62",
                "05. price": "185.14",
                "06. volume": "52455991",
                "08. previous close": "185.85"
            }
        }
        
        result = _parse_global_quote(data, "AAPL")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "symbol" in result.columns
        assert "last" in result.columns
        assert "prev_close" in result.columns
        assert "change" in result.columns
        assert "pct_change" in result.columns
    
    def test_parse_overview(self):
        """Test company overview parsing."""
        data = {
            "Symbol": "AAPL",
            "Name": "Apple Inc",
            "Description": "Apple Inc. designs, manufactures...",
            "MarketCapitalization": "3000000000000",
            "PERatio": "25.5",
            "DividendYield": "0.005",
            "EPS": "6.16"
        }
        
        result = _parse_overview(data, "AAPL")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "symbol" in result.columns
        assert "name" in result.columns
        assert "description" in result.columns
        assert "marketcapitalization" in result.columns
        assert "peratio" in result.columns
        assert "dividendyield" in result.columns
        assert "eps" in result.columns


class TestAlphaVantageErrorHandling:
    """Test Alpha Vantage error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_missing_api_key(self):
        """Test error handling when API key is missing."""
        with patch('ak_unified.adapters.alphavantage_adapter._api_key') as mock_api_key:
            mock_api_key.side_effect = AVAdapterError("Alpha Vantage API key missing")
            
            with pytest.raises(AVAdapterError) as exc_info:
                await call_alphavantage(
                    "securities.equity.us.ohlcv_daily.av",
                    {"symbol": "AAPL"}
                )
            
            assert "Alpha Vantage API key missing" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_rate_limit_error(self):
        """Test handling of rate limit errors."""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            mock_get.return_value = {
                "Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 500 calls per day. Please visit https://www.alphavantage.co/premium/ if you would like to target a higher API call frequency."
            }
            
            result = await call_alphavantage(
                "securities.equity.us.ohlcv_daily.av",
                {"symbol": "AAPL"}
            )
            
            # Should return the error message as-is
            # Should return the error message as-is
            assert isinstance(result, tuple)
            assert len(result) == 2
            assert "Note" in result[1]
            assert "rate limit" in result[1]["Note"].lower()
    
    @pytest.mark.asyncio
    async def test_invalid_json_error(self):
        """Test handling of invalid JSON responses."""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            mock_get.side_effect = AVAdapterError("Invalid JSON from Alpha Vantage")
            
            with pytest.raises(AVAdapterError) as exc_info:
                await call_alphavantage(
                    "securities.equity.us.ohlcv_daily.av",
                    {"symbol": "AAPL"}
                )
            
            assert "Invalid JSON from Alpha Vantage" in str(exc_info.value)


class TestAlphaVantageRateLimiting:
    """Test Alpha Vantage rate limiting integration."""
    
    @pytest.mark.asyncio
    async def test_rate_limit_acquisition(self):
        """Test that rate limits are acquired before making requests."""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get, \
             patch('ak_unified.adapters.alphavantage_adapter.acquire_rate_limit') as mock_rate_limit, \
             patch('ak_unified.adapters.alphavantage_adapter.acquire_daily_rate_limit') as mock_daily_limit:
            
            # Mock the rate limiting functions to return None (success)
            mock_rate_limit.return_value = None
            mock_daily_limit.return_value = None
            
            mock_get.return_value = {
                "Time Series (Daily)": {
                    "2024-01-02": {
                        "1. open": "185.59",
                        "2. high": "186.12",
                        "3. low": "183.62",
                        "4. close": "185.14",
                        "5. adjusted close": "185.14",
                        "6. volume": "52455991"
                    }
                }
            }
            
            await call_alphavantage(
                "securities.equity.us.ohlcv_daily.av",
                {"symbol": "AAPL"}
            )
            
            # Verify rate limits were acquired
            mock_rate_limit.assert_called_once_with('alphavantage')
            mock_daily_limit.assert_called_once_with('alphavantage')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])