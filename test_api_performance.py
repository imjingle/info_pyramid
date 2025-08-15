#!/usr/bin/env python3
"""
Performance test script for the new API endpoints.
"""

import asyncio
import time
import statistics
from datetime import datetime

async def test_endpoint_performance(client, endpoint, name, iterations=10):
    """Test performance of a single endpoint."""
    print(f"\n🔍 Testing {name}...")
    
    response_times = []
    success_count = 0
    
    for i in range(iterations):
        start_time = time.time()
        try:
            response = await client.get(f"http://localhost:8000{endpoint}")
            end_time = time.time()
            
            response_time = (end_time - start_time) * 1000  # Convert to milliseconds
            response_times.append(response_time)
            
            if response.status_code == 200:
                success_count += 1
                print(f"   ✅ Iteration {i+1}: {response_time:.2f}ms")
            else:
                print(f"   ❌ Iteration {i+1}: HTTP {response.status_code} ({response_time:.2f}ms)")
                
        except Exception as e:
            end_time = time.time()
            response_time = (end_time - start_time) * 1000
            print(f"   ❌ Iteration {i+1}: Error - {e} ({response_time:.2f}ms)")
    
    if response_times:
        avg_time = statistics.mean(response_times)
        min_time = min(response_times)
        max_time = max(response_times)
        std_dev = statistics.stdev(response_times) if len(response_times) > 1 else 0
        
        print(f"   📊 Performance Summary:")
        print(f"      - Success Rate: {success_count}/{iterations} ({success_count/iterations*100:.1f}%)")
        print(f"      - Average Response Time: {avg_time:.2f}ms")
        print(f"      - Min Response Time: {min_time:.2f}ms")
        print(f"      - Max Response Time: {max_time:.2f}ms")
        print(f"      - Standard Deviation: {std_dev:.2f}ms")
        
        return {
            'name': name,
            'success_rate': success_count/iterations,
            'avg_time': avg_time,
            'min_time': min_time,
            'max_time': max_time,
            'std_dev': std_dev
        }
    
    return None

async def test_api_performance():
    """Test performance of all API endpoints."""
    print("🚀 API Performance Testing")
    print("=" * 50)
    
    import httpx
    
    async with httpx.AsyncClient() as client:
        # Test basic endpoints
        print("\n1️⃣ Testing Basic Endpoints Performance...")
        basic_results = []
        
        basic_endpoints = [
            ("/rpc/datasets", "Datasets"),
            ("/rpc/rate-limits", "Rate Limits")
        ]
        
        for endpoint, name in basic_endpoints:
            result = await test_endpoint_performance(client, endpoint, name, 5)
            if result:
                basic_results.append(result)
        
        # Test Snowball endpoints
        print("\n2️⃣ Testing Snowball Endpoints Performance...")
        snowball_results = []
        
        snowball_endpoints = [
            ("/rpc/snowball/quote?symbol=000001&market=cn", "Snowball Quote"),
            ("/rpc/snowball/market_overview?market=cn", "Snowball Market Overview")
        ]
        
        for endpoint, name in snowball_endpoints:
            result = await test_endpoint_performance(client, endpoint, name, 5)
            if result:
                snowball_results.append(result)
        
        # Test EasyTrader endpoints
        print("\n3️⃣ Testing EasyTrader Endpoints Performance...")
        easytrader_results = []
        
        easytrader_endpoints = [
            ("/rpc/easytrader/account_info?broker=ht", "EasyTrader Account Info"),
            ("/rpc/easytrader/portfolio?broker=ht", "EasyTrader Portfolio")
        ]
        
        for endpoint, name in easytrader_endpoints:
            result = await test_endpoint_performance(client, endpoint, name, 5)
            if result:
                easytrader_results.append(result)
        
        # Test Earnings endpoints
        print("\n4️⃣ Testing Earnings Endpoints Performance...")
        earnings_results = []
        
        earnings_endpoints = [
            ("/rpc/earnings/calendar?market=cn", "Earnings Calendar"),
            ("/rpc/earnings/forecast?symbol=000001&market=cn", "Earnings Forecast")
        ]
        
        for endpoint, name in earnings_endpoints:
            result = await test_endpoint_performance(client, endpoint, name, 5)
            if result:
                earnings_results.append(result)
        
        # Test Fund endpoints
        print("\n5️⃣ Testing Fund Endpoints Performance...")
        fund_results = []
        
        fund_endpoints = [
            ("/rpc/fund/portfolio?fund_code=000001&market=cn", "Fund Portfolio"),
            ("/rpc/fund/top_holdings?fund_code=000001&market=cn&top_n=10", "Fund Top Holdings")
        ]
        
        for endpoint, name in fund_endpoints:
            result = await test_endpoint_performance(client, endpoint, name, 5)
            if result:
                fund_results.append(result)
        
        # Performance Summary
        print("\n📊 Performance Summary Report")
        print("=" * 50)
        
        all_results = basic_results + snowball_results + easytrader_results + earnings_results + fund_results
        
        if all_results:
            # Overall statistics
            all_avg_times = [r['avg_time'] for r in all_results]
            all_success_rates = [r['success_rate'] for r in all_results]
            
            print(f"📈 Overall Performance:")
            print(f"   - Total Endpoints Tested: {len(all_results)}")
            print(f"   - Average Response Time: {statistics.mean(all_avg_times):.2f}ms")
            print(f"   - Best Response Time: {min(all_avg_times):.2f}ms")
            print(f"   - Worst Response Time: {max(all_avg_times):.2f}ms")
            print(f"   - Average Success Rate: {statistics.mean(all_success_rates)*100:.1f}%")
            
            # Top performers
            print(f"\n🏆 Top Performers (by response time):")
            sorted_by_time = sorted(all_results, key=lambda x: x['avg_time'])
            for i, result in enumerate(sorted_by_time[:3]):
                print(f"   {i+1}. {result['name']}: {result['avg_time']:.2f}ms")
            
            # Success rate analysis
            print(f"\n✅ Success Rate Analysis:")
            high_success = [r for r in all_results if r['success_rate'] >= 0.8]
            medium_success = [r for r in all_results if 0.5 <= r['success_rate'] < 0.8]
            low_success = [r for r in all_results if r['success_rate'] < 0.5]
            
            print(f"   - High Success (≥80%): {len(high_success)} endpoints")
            print(f"   - Medium Success (50-80%): {len(medium_success)} endpoints")
            print(f"   - Low Success (<50%): {len(low_success)} endpoints")
        
        print(f"\n✅ Performance Testing Complete!")

if __name__ == "__main__":
    asyncio.run(test_api_performance())