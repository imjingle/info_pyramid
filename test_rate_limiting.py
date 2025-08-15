#!/usr/bin/env python3
"""
Test script for rate limiting functionality
"""

import asyncio
import time
from src.ak_unified.rate_limiter import acquire_rate_limit, acquire_daily_rate_limit, get_rate_limit_status


async def test_alphavantage_limits():
    """Test Alpha Vantage rate limiting"""
    print("Testing Alpha Vantage rate limiting...")
    
    start_time = time.time()
    
    # Test multiple rapid requests
    for i in range(3):
        print(f"Request {i+1}: Acquiring Alpha Vantage rate limit...")
        await acquire_rate_limit('alphavantage')
        await acquire_daily_rate_limit('alphavantage')
        print(f"Request {i+1}: Rate limit acquired")
    
    elapsed = time.time() - start_time
    print(f"Completed 3 requests in {elapsed:.2f} seconds")
    
    # Get status
    status = await get_rate_limit_status()
    print(f"Alpha Vantage status: {status.get('alphavantage', 'Not found')}")


async def test_akshare_limits():
    """Test AkShare rate limiting"""
    print("\nTesting AkShare rate limiting...")
    
    vendors = ['eastmoney', 'sina', 'tencent', 'ths']
    
    for vendor in vendors:
        print(f"Testing {vendor} vendor...")
        start_time = time.time()
        
        # Test multiple rapid requests
        for i in range(2):
            print(f"  Request {i+1}: Acquiring {vendor} rate limit...")
            await acquire_rate_limit('akshare', vendor)
            print(f"  Request {i+1}: Rate limit acquired")
        
        elapsed = time.time() - start_time
        print(f"  Completed 2 requests in {elapsed:.2f} seconds")
    
    # Get status
    status = await get_rate_limit_status()
    for vendor in vendors:
        key = f'akshare_{vendor}'
        if key in status:
            print(f"{vendor} status: {status[key]}")


async def test_concurrent_requests():
    """Test concurrent rate limiting"""
    print("\nTesting concurrent rate limiting...")
    
    async def make_request(request_id: int):
        print(f"Request {request_id}: Starting...")
        await acquire_rate_limit('alphavantage')
        await acquire_daily_rate_limit('alphavantage')
        print(f"Request {request_id}: Completed")
        return request_id
    
    # Start 5 concurrent requests
    tasks = [make_request(i) for i in range(5)]
    start_time = time.time()
    
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start_time
    
    print(f"Completed {len(results)} concurrent requests in {elapsed:.2f} seconds")
    print(f"Results: {results}")


async def test_rate_limit_status():
    """Test rate limit status endpoint"""
    print("\nTesting rate limit status...")
    
    try:
        status = await get_rate_limit_status()
        print("Rate limiting enabled:", bool(status))
        print("Number of limiters:", len(status))
        
        for key, limiter_info in status.items():
            print(f"\n{key}:")
            for info_key, info_value in limiter_info.items():
                print(f"  {info_key}: {info_value}")
                
    except Exception as e:
        print(f"Error getting status: {e}")


async def main():
    """Main test function"""
    print("Starting rate limiting tests...")
    print("=" * 50)
    
    try:
        await test_alphavantage_limits()
        await test_akshare_limits()
        await test_concurrent_requests()
        await test_rate_limit_status()
        
        print("\n" + "=" * 50)
        print("All tests completed successfully!")
        
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())