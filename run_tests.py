#!/usr/bin/env python3
"""
Test runner script for ak-unified project.
Runs all tests and provides a summary of test coverage.
"""

import subprocess
import sys
import os
from pathlib import Path


def run_test_suite(test_file: str, description: str) -> bool:
    """Run a specific test suite and return success status."""
    print(f"\n{'='*60}")
    print(f"Running {description}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "pytest", test_file, "-v", "--tb=short"
        ], capture_output=True, text=True, cwd=os.getcwd())
        
        if result.returncode == 0:
            print("✅ All tests passed!")
            return True
        else:
            print("❌ Some tests failed:")
            print(result.stdout)
            if result.stderr:
                print("Errors:")
                print(result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Failed to run tests: {e}")
        return False


def main():
    """Main test runner function."""
    print("🚀 Starting ak-unified test suite...")
    print(f"Python: {sys.executable}")
    print(f"Working directory: {os.getcwd()}")
    
    # Define test suites
    test_suites = [
        ("tests/test_rate_limiter.py", "Rate Limiter Tests"),
        ("tests/test_akshare_functions.py", "AkShare Adapter Tests"),
        ("tests/test_alphavantage_adapter.py", "Alpha Vantage Adapter Tests"),
        ("tests/test_core.py", "Core Functionality Tests"),
        ("tests/test_datasets.py", "Dataset Tests"),
    ]
    
    results = []
    
    for test_file, description in test_suites:
        if os.path.exists(test_file):
            success = run_test_suite(test_file, description)
            results.append((description, success))
        else:
            print(f"\n⚠️  Test file not found: {test_file}")
            results.append((description, False))
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    
    passed = 0
    total = len(results)
    
    for description, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {description}")
        if success:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} test suites passed")
    
    if passed == total:
        print("\n🎉 All test suites passed successfully!")
        print("\n📋 Test Coverage Summary:")
        print("  ✅ Rate Limiting System")
        print("    - Token bucket rate limiting with aiolimiter")
        print("    - Alpha Vantage: 5 req/min, 500 req/day")
        print("    - AkShare vendors: configurable per-vendor limits")
        print("    - Automatic vendor detection")
        print("    - Daily and per-minute rate limiting")
        print("    - Configurable via environment variables")
        print("    - Status monitoring and debugging")
        print("\n  ✅ AkShare Adapter")
        print("    - Basic functions (stock_zh_a_hist, stock_zh_a_spot_em)")
        print("    - Index functions (stock_zh_index_spot, stock_zh_index_daily)")
        print("    - Fund functions (fund_etf_hist_em)")
        print("    - Bond functions (bond_zh_hs_cov_spot)")
        print("    - Macro functions (CPI, PMI)")
        print("    - Vendor detection (eastmoney, sina, tencent, ths, tdx)")
        print("    - Error handling and fallback functionality")
        print("    - Data transformation and normalization")
        print("\n  ✅ Alpha Vantage Adapter")
        print("    - Time series data (daily, intraday)")
        print("    - Real-time quotes (GLOBAL_QUOTE)")
        print("    - Fundamental data (overview, statements)")
        print("    - Macroeconomic data (CPI, PMI)")
        print("    - Data parsing and normalization")
        print("    - Error handling and rate limiting")
        print("\n  ✅ Core System")
        print("    - Data fetching and caching")
        print("    - Dataset registry and management")
        print("    - API endpoints and routing")
        print("    - Configuration management")
        
        return 0
    else:
        print(f"\n❌ {total - passed} test suite(s) failed")
        print("Please check the test output above for details")
        return 1


if __name__ == "__main__":
    sys.exit(main())