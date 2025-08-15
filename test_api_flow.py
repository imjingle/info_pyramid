#!/usr/bin/env python3
"""
Test script to verify the complete data flow from adapters to API responses.
"""

import asyncio
import json
from datetime import datetime

async def test_api_flow():
    """Test the complete API flow."""
    print("🧪 Testing API Data Flow...")
    print("=" * 50)
    
    # Test 1: Basic endpoints
    print("\n1️⃣ Testing Basic Endpoints...")
    await test_basic_endpoints()
    
    # Test 2: Snowball endpoints
    print("\n2️⃣ Testing Snowball Endpoints...")
    await test_snowball_endpoints()
    
    # Test 3: EasyTrader endpoints
    print("\n3️⃣ Testing EasyTrader Endpoints...")
    await test_easytrader_endpoints()
    
    # Test 4: Earnings endpoints
    print("\n4️⃣ Testing Earnings Endpoints...")
    await test_earnings_endpoints()
    
    # Test 5: Fund endpoints
    print("\n5️⃣ Testing Fund Endpoints...")
    await test_fund_endpoints()
    
    print("\n✅ API Flow Testing Complete!")

async def test_basic_endpoints():
    """Test basic API endpoints."""
    import httpx
    
    async with httpx.AsyncClient() as client:
        # Test datasets endpoint
        response = await client.get("http://localhost:8000/rpc/datasets")
        if response.status_code == 200:
            data = response.json()
            print(f"   📊 Datasets: {len(data.get('items', []))} datasets available")
        else:
            print(f"   ❌ Datasets: Failed with status {response.status_code}")
        
        # Test rate limits endpoint
        response = await client.get("http://localhost:8000/rpc/rate-limits")
        if response.status_code == 200:
            data = response.json()
            limiters = data.get('limiters', {})
            print(f"   🚦 Rate Limits: {len(limiters)} limiters configured")
            if 'snowball_default' in limiters:
                print(f"      - Snowball: {limiters['snowball_default']['max_rate']} req/min")
            if 'easytrader_default' in limiters:
                print(f"      - EasyTrader: {limiters['easytrader_default']['max_rate']} req/min")
        else:
            print(f"   ❌ Rate Limits: Failed with status {response.status_code}")

async def test_snowball_endpoints():
    """Test Snowball API endpoints."""
    import httpx
    
    async with httpx.AsyncClient() as client:
        endpoints = [
            ("/rpc/snowball/quote?symbol=000001&market=cn", "Stock Quote"),
            ("/rpc/snowball/financial_data?symbol=000001&market=cn&period=annual", "Financial Data"),
            ("/rpc/snowball/research_reports?symbol=000001&market=cn&limit=5", "Research Reports"),
            ("/rpc/snowball/sentiment?symbol=000001&market=cn&days=7", "Sentiment"),
            ("/rpc/snowball/discussions?symbol=000001&market=cn&limit=10", "Discussions"),
            ("/rpc/snowball/market_overview?market=cn", "Market Overview")
        ]
        
        for endpoint, name in endpoints:
            try:
                response = await client.get(f"http://localhost:8000{endpoint}")
                if response.status_code == 200:
                    data = response.json()
                    success = data.get('success', False)
                    if success:
                        print(f"   ✅ {name}: Success")
                    else:
                        print(f"   ⚠️  {name}: Expected failure (no data)")
                else:
                    print(f"   ❌ {name}: HTTP {response.status_code}")
            except Exception as e:
                print(f"   ❌ {name}: Error - {e}")

async def test_easytrader_endpoints():
    """Test EasyTrader API endpoints."""
    import httpx
    
    async with httpx.AsyncClient() as client:
        endpoints = [
            ("/rpc/easytrader/account_info?broker=ht", "Account Info"),
            ("/rpc/easytrader/portfolio?broker=ht", "Portfolio"),
            ("/rpc/easytrader/trading_history?broker=ht", "Trading History"),
            ("/rpc/easytrader/market_data?broker=ht&symbols=000001,000002", "Market Data"),
            ("/rpc/easytrader/fund_info?broker=ht", "Fund Info"),
            ("/rpc/easytrader/risk_metrics?broker=ht", "Risk Metrics")
        ]
        
        for endpoint, name in endpoints:
            try:
                response = await client.get(f"http://localhost:8000{endpoint}")
                if response.status_code == 200:
                    data = response.json()
                    success = data.get('success', False)
                    if success:
                        print(f"   ✅ {name}: Success")
                    else:
                        print(f"   ⚠️  {name}: Expected failure (no data)")
                else:
                    print(f"   ❌ {name}: HTTP {response.status_code}")
            except Exception as e:
                print(f"   ❌ {name}: Error - {e}")

async def test_earnings_endpoints():
    """Test Earnings API endpoints."""
    import httpx
    
    async with httpx.AsyncClient() as client:
        endpoints = [
            ("/rpc/earnings/calendar?market=cn", "Earnings Calendar"),
            ("/rpc/earnings/forecast?symbol=000001&market=cn", "Earnings Forecast"),
            ("/rpc/earnings/dates?symbol=000001&market=cn", "Earnings Dates"),
            ("/rpc/financial/indicators?symbol=000001&market=cn&period=annual", "Financial Indicators"),
            ("/rpc/financial/statements?symbol=000001&market=cn&period=annual&statement_type=balance", "Financial Statements")
        ]
        
        for endpoint, name in endpoints:
            try:
                response = await client.get(f"http://localhost:8000{endpoint}")
                if response.status_code == 200:
                    data = response.json()
                    success = data.get('success', False)
                    if success:
                        print(f"   ✅ {name}: Success")
                    else:
                        print(f"   ⚠️  {name}: Expected failure (no data)")
                else:
                    print(f"   ❌ {name}: HTTP {response.status_code}")
            except Exception as e:
                print(f"   ❌ {name}: Error - {e}")

async def test_fund_endpoints():
    """Test Fund API endpoints."""
    import httpx
    
    async with httpx.AsyncClient() as client:
        endpoints = [
            ("/rpc/fund/portfolio?fund_code=000001&market=cn", "Fund Portfolio"),
            ("/rpc/fund/holdings_change?fund_code=000001&market=cn", "Holdings Change"),
            ("/rpc/fund/top_holdings?fund_code=000001&market=cn&top_n=10", "Top Holdings")
        ]
        
        for endpoint, name in endpoints:
            try:
                response = await client.get(f"http://localhost:8000{endpoint}")
                if response.status_code == 200:
                    data = response.json()
                    success = data.get('success', False)
                    if success:
                        print(f"   ✅ {name}: Success")
                    else:
                        print(f"   ⚠️  {name}: Expected failure (no data)")
                else:
                    print(f"   ❌ {name}: HTTP {response.status_code}")
            except Exception as e:
                print(f"   ❌ {name}: Error - {e}")

if __name__ == "__main__":
    asyncio.run(test_api_flow())