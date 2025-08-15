#!/usr/bin/env python3
"""
Test script to verify error handling in various scenarios.
"""

import asyncio
import json
from datetime import datetime

async def test_error_handling():
    """Test error handling in various scenarios."""
    print("🧪 Testing Error Handling...")
    print("=" * 50)
    
    # Test 1: Invalid parameters
    print("\n1️⃣ Testing Invalid Parameters...")
    await test_invalid_parameters()
    
    # Test 2: Missing dependencies
    print("\n2️⃣ Testing Missing Dependencies...")
    await test_missing_dependencies()
    
    # Test 3: Rate limiting
    print("\n3️⃣ Testing Rate Limiting...")
    await test_rate_limiting()
    
    # Test 4: Edge cases
    print("\n4️⃣ Testing Edge Cases...")
    await test_edge_cases()
    
    print("\n✅ Error Handling Testing Complete!")

async def test_invalid_parameters():
    """Test error handling with invalid parameters."""
    import httpx
    
    async with httpx.AsyncClient() as client:
        # Test 1: Missing required parameters
        print("   🔍 Testing missing required parameters...")
        
        # Test Snowball quote without symbol
        try:
            response = await client.get("http://localhost:8000/rpc/snowball/quote")
            print(f"      - Snowball quote (no symbol): HTTP {response.status_code}")
            if response.status_code == 422:  # Validation error
                print("      ✅ Correctly returned validation error")
            else:
                print("      ❌ Unexpected response")
        except Exception as e:
            print(f"      ❌ Error: {e}")
        
        # Test EasyTrader without broker
        try:
            response = await client.get("http://localhost:8000/rpc/easytrader/account_info")
            print(f"      - EasyTrader account info (no broker): HTTP {response.status_code}")
            if response.status_code == 422:  # Validation error
                print("      ✅ Correctly returned validation error")
            else:
                print("      ❌ Unexpected response")
        except Exception as e:
            print(f"      ❌ Error: {e}")
        
        # Test 2: Invalid parameter values
        print("   🔍 Testing invalid parameter values...")
        
        # Test with invalid market code
        try:
            response = await client.get("http://localhost:8000/rpc/snowball/quote?symbol=000001&market=invalid")
            print(f"      - Snowball quote (invalid market): HTTP {response.status_code}")
            if response.status_code == 200:  # Should still work but return empty data
                data = response.json()
                if not data.get('success', True):
                    print("      ✅ Correctly handled invalid market")
                else:
                    print("      ⚠️  Accepted invalid market")
            else:
                print("      ❌ Unexpected response")
        except Exception as e:
            print(f"      ❌ Error: {e}")

async def test_missing_dependencies():
    """Test error handling when dependencies are missing."""
    import httpx
    
    async with httpx.AsyncClient() as client:
        print("   🔍 Testing missing dependencies...")
        
        # Test Snowball endpoints (pysnowball not installed)
        try:
            response = await client.get("http://localhost:8000/rpc/snowball/quote?symbol=000001&market=cn")
            if response.status_code == 200:
                data = response.json()
                if not data.get('success', True) and data.get('quote') is None:
                    print("      ✅ Snowball: Correctly handled missing dependency")
                else:
                    print("      ❌ Snowball: Unexpected response")
            else:
                print(f"      ❌ Snowball: HTTP {response.status_code}")
        except Exception as e:
            print(f"      ❌ Snowball: Error - {e}")
        
        # Test EasyTrader endpoints (easytrader not installed)
        try:
            response = await client.get("http://localhost:8000/rpc/easytrader/account_info?broker=ht")
            if response.status_code == 200:
                data = response.json()
                if not data.get('success', True) and data.get('account_info') is None:
                    print("      ✅ EasyTrader: Correctly handled missing dependency")
                else:
                    print("      ❌ EasyTrader: Unexpected response")
            else:
                print(f"      ❌ EasyTrader: HTTP {response.status_code}")
        except Exception as e:
            print(f"      ❌ EasyTrader: Error - {e}")
        
        # Test Earnings endpoints (akshare not installed)
        try:
            response = await client.get("http://localhost:8000/rpc/earnings/calendar?market=cn")
            if response.status_code == 200:
                data = response.json()
                if data.get('success', False) and data.get('data') == []:
                    print("      ✅ Earnings: Correctly handled missing dependency")
                else:
                    print("      ❌ Earnings: Unexpected response")
            else:
                print(f"      ❌ Earnings: HTTP {response.status_code}")
        except Exception as e:
            print(f"      ❌ Earnings: Error - {e}")

async def test_rate_limiting():
    """Test rate limiting behavior."""
    import httpx
    
    async with httpx.AsyncClient() as client:
        print("   🔍 Testing rate limiting...")
        
        # Test rapid requests to see if rate limiting is working
        print("      - Testing rapid requests to Snowball endpoint...")
        
        start_time = time.time()
        responses = []
        
        for i in range(10):
            try:
                response = await client.get("http://localhost:8000/rpc/snowball/quote?symbol=000001&market=cn")
                responses.append(response.status_code)
                if i % 3 == 0:  # Print every 3rd response
                    print(f"        Request {i+1}: HTTP {response.status_code}")
            except Exception as e:
                print(f"        Request {i+1}: Error - {e}")
                responses.append(0)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"      - Completed 10 requests in {total_time:.2f}s")
        print(f"      - Success rate: {responses.count(200)}/10")
        
        # Check if rate limiting is working (should see some 429 responses if rate limited)
        if 429 in responses:
            print("      ✅ Rate limiting is working (saw 429 responses)")
        else:
            print("      ⚠️  No rate limiting observed (all requests succeeded)")

async def test_edge_cases():
    """Test edge cases and boundary conditions."""
    import httpx
    
    async with httpx.AsyncClient() as client:
        print("   🔍 Testing edge cases...")
        
        # Test 1: Very long symbol
        try:
            long_symbol = "A" * 1000  # Very long symbol
            response = await client.get(f"http://localhost:8000/rpc/snowball/quote?symbol={long_symbol}&market=cn")
            print(f"      - Very long symbol: HTTP {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                if not data.get('success', True):
                    print("      ✅ Correctly handled very long symbol")
                else:
                    print("      ⚠️  Accepted very long symbol")
            else:
                print("      ❌ Unexpected response")
        except Exception as e:
            print(f"      ❌ Error: {e}")
        
        # Test 2: Special characters in parameters
        try:
            special_symbol = "000001!@#$%^&*()"
            response = await client.get(f"http://localhost:8000/rpc/snowball/quote?symbol={special_symbol}&market=cn")
            print(f"      - Special characters in symbol: HTTP {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                if not data.get('success', True):
                    print("      ✅ Correctly handled special characters")
                else:
                    print("      ⚠️  Accepted special characters")
            else:
                print("      ❌ Unexpected response")
        except Exception as e:
            print(f"      ❌ Error: {e}")
        
        # Test 3: Empty parameters
        try:
            response = await client.get("http://localhost:8000/rpc/snowball/quote?symbol=&market=cn")
            print(f"      - Empty symbol: HTTP {response.status_code}")
            if response.status_code == 422:  # Validation error
                print("      ✅ Correctly handled empty symbol")
            else:
                print("      ❌ Unexpected response")
        except Exception as e:
            print(f"      ❌ Error: {e}")

if __name__ == "__main__":
    import time
    asyncio.run(test_error_handling())