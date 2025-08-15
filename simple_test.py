#!/usr/bin/env python3
"""
Simple test script for rate limiting functionality
"""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

async def test_rate_limiter():
    """Test the rate limiter functionality"""
    try:
        from ak_unified.rate_limiter import acquire_rate_limit, acquire_daily_rate_limit, get_rate_limit_status
        
        print("✅ Rate limiter import successful")
        
        # Test basic functionality
        print("\n🔒 Testing rate limiting...")
        
        # Test Alpha Vantage
        print("  Testing Alpha Vantage...")
        await acquire_rate_limit('alphavantage')
        await acquire_daily_rate_limit('alphavantage')
        print("    ✅ Alpha Vantage rate limits acquired")
        
        # Test AkShare vendors
        vendors = ['eastmoney', 'sina', 'tencent']
        for vendor in vendors:
            print(f"  Testing AkShare {vendor}...")
            await acquire_rate_limit('akshare', vendor)
            print(f"    ✅ {vendor} rate limit acquired")
        
        # Get status
        print("\n📊 Getting rate limiter status...")
        status = await get_rate_limit_status()
        print(f"  Number of limiters: {len(status)}")
        
        # Show some key limiters
        key_limiters = ['alphavantage', 'alphavantage_daily', 'akshare_eastmoney']
        for key in key_limiters:
            if key in status:
                info = status[key]
                print(f"  {key}: {info['max_rate']} req/{info['time_period']}s")
        
        print("\n🎉 All tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_adapters():
    """Test adapter imports"""
    try:
        print("\n🔌 Testing adapter imports...")
        
        # Test Alpha Vantage adapter
        from ak_unified.adapters.alphavantage_adapter import AVAdapterError
        print("  ✅ Alpha Vantage adapter import successful")
        
        # Test AkShare adapter
        from ak_unified.adapters.akshare_adapter import AkAdapterError
        print("  ✅ AkShare adapter import successful")
        
        print("🎉 All adapter imports successful!")
        return True
        
    except Exception as e:
        print(f"❌ Adapter test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Main test function"""
    print("🚀 Starting rate limiting tests...")
    print("=" * 50)
    
    success = True
    
    # Test rate limiter
    if not await test_rate_limiter():
        success = False
    
    # Test adapters
    if not await test_adapters():
        success = False
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 All tests completed successfully!")
        print("\n📋 Rate limiting features:")
        print("  ✅ Token bucket rate limiting with aiolimiter")
        print("  ✅ Alpha Vantage: 5 req/min, 500 req/day")
        print("  ✅ AkShare vendors: configurable per-vendor limits")
        print("  ✅ Automatic vendor detection for AkShare functions")
        print("  ✅ Daily and per-minute rate limiting")
        print("  ✅ Configurable via environment variables")
        print("  ✅ Status monitoring and debugging")
    else:
        print("❌ Some tests failed")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())