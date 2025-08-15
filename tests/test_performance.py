"""
Performance benchmark tests for AK Unified.
Tests system performance under various load conditions.
"""

import pytest
import asyncio
import time
from unittest.mock import patch, MagicMock, AsyncMock
from ak_unified.rate_limiter import acquire_rate_limit, acquire_daily_rate_limit
from ak_unified.adapters.akshare_adapter import call_akshare
from ak_unified.adapters.alphavantage_adapter import call_alphavantage


class TestRateLimiterPerformance:
    """Test rate limiter performance under load."""
    
    @pytest.mark.asyncio
    async def test_concurrent_rate_limit_requests(self):
        """Test performance of concurrent rate limit requests."""
        with patch('ak_unified.rate_limiter.settings') as mock_settings, \
             patch('ak_unified.rate_limiter.rate_limiter') as mock_manager:
            
            mock_settings.RATE_LIMIT_ENABLED = True
            mock_manager.acquire = AsyncMock(return_value=None)
            
            # Test concurrent requests
            start_time = time.time()
            
            # Create 100 concurrent requests
            tasks = [acquire_rate_limit('alphavantage') for _ in range(100)]
            await asyncio.gather(*tasks)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should complete within reasonable time (less than 1 second)
            assert duration < 1.0
            assert mock_manager.acquire.call_count == 100
    
    @pytest.mark.asyncio
    async def test_rate_limiter_throughput(self):
        """Test rate limiter throughput under sustained load."""
        with patch('ak_unified.rate_limiter.settings') as mock_settings, \
             patch('ak_unified.rate_limiter.rate_limiter') as mock_manager:
            
            mock_settings.RATE_LIMIT_ENABLED = True
            mock_manager.acquire = AsyncMock(return_value=None)
            
            # Test sustained load for 1 second
            start_time = time.time()
            request_count = 0
            
            while time.time() - start_time < 1.0:
                await acquire_rate_limit('alphavantage')
                request_count += 1
            
            # Should handle at least 100 requests per second
            assert request_count >= 100
    
    @pytest.mark.benchmark
    def test_rate_limiter_benchmark(self, benchmark):
        """Benchmark rate limiter performance."""
        with patch('ak_unified.rate_limiter.settings') as mock_settings, \
             patch('ak_unified.rate_limiter.rate_limiter') as mock_manager:
            
            mock_settings.RATE_LIMIT_ENABLED = True
            mock_manager.acquire = AsyncMock(return_value=None)
            
            async def benchmark_func():
                await acquire_rate_limit('alphavantage')
            
            # Run benchmark
            result = benchmark(asyncio.run, benchmark_func())
            assert result is None


class TestAdapterPerformance:
    """Test adapter performance under load."""
    
    @pytest.mark.asyncio
    async def test_akshare_concurrent_requests(self):
        """Test AkShare adapter performance under concurrent load."""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_a_hist.return_value = MagicMock()
            mock_import.return_value = mock_ak
            
            # Test concurrent requests
            start_time = time.time()
            
            tasks = [
                call_akshare(['stock_zh_a_hist'], {'symbol': '000001'}, function_name='stock_zh_a_hist')
                for _ in range(50)
            ]
            results = await asyncio.gather(*tasks)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should complete within reasonable time
            assert duration < 2.0
            assert len(results) == 50
    
    @pytest.mark.asyncio
    async def test_alphavantage_concurrent_requests(self):
        """Test Alpha Vantage adapter performance under concurrent load."""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get, \
             patch('ak_unified.adapters.alphavantage_adapter.acquire_rate_limit') as mock_rate_limit, \
             patch('ak_unified.adapters.alphavantage_adapter.acquire_daily_rate_limit') as mock_daily_limit:
            
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
            mock_rate_limit.return_value = None
            mock_daily_limit.return_value = None
            
            # Test concurrent requests
            start_time = time.time()
            
            tasks = [
                call_alphavantage("securities.equity.us.ohlcv_daily.av", {"symbol": "AAPL"})
                for _ in range(20)  # Lower number due to rate limits
            ]
            results = await asyncio.gather(*tasks)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should complete within reasonable time
            assert duration < 2.0
            assert len(results) == 20
    
    @pytest.mark.benchmark
    def test_akshare_benchmark(self, benchmark):
        """Benchmark AkShare adapter performance."""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_a_hist.return_value = MagicMock()
            mock_import.return_value = mock_ak
            
            async def benchmark_func():
                return await call_akshare(['stock_zh_a_hist'], {'symbol': '000001'}, function_name='stock_zh_a_hist')
            
            # Run benchmark
            result = benchmark(asyncio.run, benchmark_func())
            assert isinstance(result, tuple)
            assert len(result) == 2


class TestDataProcessingPerformance:
    """Test data processing performance."""
    
    @pytest.mark.asyncio
    async def test_large_dataframe_processing(self):
        """Test performance with large DataFrames."""
        import pandas as pd
        
        # Create large DataFrame
        large_df = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=10000, freq='D'),
            'open': [100.0 + i * 0.01 for i in range(10000)],
            'high': [101.0 + i * 0.01 for i in range(10000)],
            'low': [99.0 + i * 0.01 for i in range(10000)],
            'close': [100.5 + i * 0.01 for i in range(10000)],
            'volume': [1000000 + i * 100 for i in range(10000)]
        })
        
        start_time = time.time()
        
        # Process large DataFrame
        processed_df = large_df.copy()
        processed_df['symbol'] = 'TEST'
        processed_df['returns'] = processed_df['close'].pct_change()
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should process 10k rows within reasonable time
        assert duration < 1.0
        assert len(processed_df) == 10000
        assert 'symbol' in processed_df.columns
        assert 'returns' in processed_df.columns
    
    @pytest.mark.benchmark
    def test_dataframe_operations_benchmark(self, benchmark):
        """Benchmark DataFrame operations."""
        import pandas as pd
        
        # Create test DataFrame
        df = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=1000, freq='D'),
            'close': [100.0 + i * 0.01 for i in range(1000)]
        })
        
        def benchmark_func():
            df_copy = df.copy()
            df_copy['returns'] = df_copy['close'].pct_change()
            df_copy['ma_20'] = df_copy['close'].rolling(20).mean()
            return df_copy
        
        # Run benchmark
        result = benchmark(benchmark_func)
        assert len(result) == 1000
        assert 'returns' in result.columns
        assert 'ma_20' in result.columns


class TestMemoryPerformance:
    """Test memory usage and performance."""
    
    @pytest.mark.asyncio
    async def test_memory_efficient_processing(self):
        """Test memory-efficient data processing."""
        import pandas as pd
        import gc
        
        # Create large dataset
        large_df = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=50000, freq='D'),
            'value': [i * 0.01 for i in range(50000)]
        })
        
        # Get initial memory usage
        gc.collect()
        initial_memory = large_df.memory_usage(deep=True).sum()
        
        # Process data in chunks
        chunk_size = 1000
        processed_chunks = []
        
        for i in range(0, len(large_df), chunk_size):
            chunk = large_df.iloc[i:i+chunk_size].copy()
            chunk['processed'] = chunk['value'] * 2
            processed_chunks.append(chunk)
        
        # Combine chunks
        result_df = pd.concat(processed_chunks, ignore_index=True)
        
        # Get final memory usage
        gc.collect()
        final_memory = result_df.memory_usage(deep=True).sum()
        
        # Memory usage should be reasonable
        assert final_memory < initial_memory * 2  # Should not double memory usage
        assert len(result_df) == 50000
        assert 'processed' in result_df.columns
    
    @pytest.mark.benchmark
    def test_memory_benchmark(self, benchmark):
        """Benchmark memory usage."""
        import pandas as pd
        
        def benchmark_func():
            # Create and process DataFrame
            df = pd.DataFrame({
                'value': [i * 0.01 for i in range(10000)]
            })
            df['processed'] = df['value'] * 2
            df['filtered'] = df[df['processed'] > 10]
            return df
        
        # Run benchmark
        result = benchmark(benchmark_func)
        assert len(result) > 0
        assert 'processed' in result.columns
        assert 'filtered' in result.columns


class TestNetworkPerformance:
    """Test network-related performance."""
    
    @pytest.mark.asyncio
    async def test_network_timeout_handling(self):
        """Test performance under network timeouts."""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            # Simulate network timeout
            mock_get.side_effect = asyncio.TimeoutError("Network timeout")
            
            start_time = time.time()
            
            try:
                await call_alphavantage("securities.equity.us.ohlcv_daily.av", {"symbol": "AAPL"})
            except asyncio.TimeoutError:
                pass
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Timeout should be handled quickly
            assert duration < 0.1
    
    @pytest.mark.asyncio
    async def test_network_retry_performance(self):
        """Test performance with network retries."""
        with patch('ak_unified.adapters.alphavantage_adapter._get') as mock_get:
            # Simulate network failures with eventual success
            mock_get.side_effect = [
                Exception("Network error"),
                Exception("Network error"),
                {"Time Series (Daily)": {"2024-01-02": {"1. open": "185.59"}}}
            ]
            
            start_time = time.time()
            
            # Should eventually succeed
            result = await call_alphavantage("securities.equity.us.ohlcv_daily.av", {"symbol": "AAPL"})
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should complete within reasonable time despite retries
            assert duration < 1.0
            assert isinstance(result, tuple)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--benchmark-only"])