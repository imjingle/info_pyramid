"""
Comprehensive tests for rate limiter functionality.
These tests validate the rate limiting system for different data sources.
"""

import pytest
import asyncio
import time
from unittest.mock import patch, MagicMock, AsyncMock
from ak_unified.rate_limiter import (
    RateLimiterManager,
    acquire_rate_limit,
    acquire_daily_rate_limit,
    get_rate_limit_status,
    rate_limiter
)


class TestRateLimiterManager:
    """Test the RateLimiterManager class."""
    
    def test_initialization(self):
        """Test rate limiter manager initialization."""
        manager = RateLimiterManager()
        assert manager._limiters == {}
        assert manager._daily_limiters == {}
        assert manager._initialized is False
    
    @pytest.mark.asyncio
    async def test_ensure_initialized(self):
        """Test rate limiter initialization."""
        manager = RateLimiterManager()
        
        # Mock settings
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            mock_settings.AV_RATE_LIMIT_PER_MIN = 5
            mock_settings.AV_RATE_LIMIT_PER_DAY = 500
            mock_settings.AKSHARE_EASTMONEY_RATE_LIMIT = 60
            mock_settings.AKSHARE_SINA_RATE_LIMIT = 100
            mock_settings.AKSHARE_DEFAULT_RATE_LIMIT = 30
            
            await manager._ensure_initialized()
            
            assert manager._initialized is True
            assert 'alphavantage' in manager._limiters
            assert 'alphavantage' in manager._daily_limiters
            assert 'akshare_eastmoney' in manager._limiters
            assert 'akshare_sina' in manager._limiters
            assert 'akshare_default' in manager._limiters
    
    @pytest.mark.asyncio
    async def test_disabled_rate_limiting(self):
        """Test rate limiting when disabled."""
        manager = RateLimiterManager()
        
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = False
            
            await manager._ensure_initialized()
            
            assert manager._initialized is True
            assert len(manager._limiters) == 0
            assert len(manager._daily_limiters) == 0
    
    def test_get_limiter_key(self):
        """Test limiter key generation."""
        manager = RateLimiterManager()
        
        # Test Alpha Vantage
        assert manager._get_limiter_key('alphavantage') == 'alphavantage'
        
        # Test AkShare with vendor
        assert manager._get_limiter_key('akshare', 'eastmoney') == 'akshare_eastmoney'
        assert manager._get_limiter_key('akshare', 'sina') == 'akshare_sina'
        assert manager._get_limiter_key('akshare', 'unknown') == 'akshare_default'
        assert manager._get_limiter_key('akshare') == 'akshare_default'
        
        # Test unknown source
        assert manager._get_limiter_key('unknown') == 'unknown_default'


class TestRateLimiterAcquisition:
    """Test rate limit acquisition functionality."""
    
    @pytest.mark.asyncio
    async def test_acquire_rate_limit_alphavantage(self):
        """Test acquiring Alpha Vantage rate limit."""
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            
            # Mock the rate limiter manager
            with patch('ak_unified.rate_limiter.rate_limiter') as mock_manager:
                mock_manager.acquire = AsyncMock(return_value=None)
                
                await acquire_rate_limit('alphavantage')
                
                mock_manager.acquire.assert_called_once_with('alphavantage')
    
    @pytest.mark.asyncio
    async def test_acquire_rate_limit_akshare(self):
        """Test acquiring AkShare rate limit with vendor."""
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            
            with patch('ak_unified.rate_limiter.rate_limiter') as mock_manager:
                mock_manager.acquire = AsyncMock(return_value=None)
                
                await acquire_rate_limit('akshare', 'eastmoney')
                
                mock_manager.acquire.assert_called_once_with('akshare', 'eastmoney')
    
    @pytest.mark.asyncio
    async def test_acquire_daily_rate_limit(self):
        """Test acquiring daily rate limit."""
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            
            with patch('ak_unified.rate_limiter.rate_limiter') as mock_manager:
                mock_manager.acquire_daily = AsyncMock(return_value=None)
                
                await acquire_daily_rate_limit('alphavantage')
                
                mock_manager.acquire_daily.assert_called_once_with('alphavantage')
    
    @pytest.mark.asyncio
    async def test_rate_limit_disabled(self):
        """Test rate limiting when disabled."""
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = False
            
            with patch('ak_unified.rate_limiter.rate_limiter') as mock_manager:
                mock_manager.acquire = AsyncMock(return_value=None)
                mock_manager.acquire_daily = AsyncMock(return_value=None)
                
                await acquire_rate_limit('alphavantage')
                await acquire_daily_rate_limit('alphavantage')
                
                # Should not call acquire methods when disabled
                mock_manager.acquire.assert_not_called()
                mock_manager.acquire_daily.assert_not_called()


class TestRateLimiterStatus:
    """Test rate limiter status functionality."""
    
    @pytest.mark.asyncio
    async def test_get_rate_limit_status(self):
        """Test getting rate limiter status."""
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            
            with patch('ak_unified.rate_limiter.rate_limiter') as mock_manager:
                mock_status = {
                    'alphavantage': {
                        'max_rate': 5,
                        'time_period': 60.0,
                        'rate_per_sec': 0.08333333333333333,
                        'level': 4.5,
                        'has_capacity': True
                    },
                    'akshare_eastmoney': {
                        'max_rate': 60,
                        'time_period': 60.0,
                        'rate_per_sec': 1.0,
                        'level': 45.0,
                        'has_capacity': True
                    }
                }
                mock_manager.get_limiter_status = AsyncMock(return_value=mock_status)
                
                status = await get_rate_limit_status()
                
                assert status == mock_status
                mock_manager.get_limiter_status.assert_called_once()


class TestRateLimiterIntegration:
    """Test rate limiter integration with real limiters."""
    
    @pytest.mark.asyncio
    async def test_real_rate_limiting(self):
        """Test actual rate limiting behavior."""
        # Create a fresh manager for testing
        manager = RateLimiterManager()
        
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            mock_settings.AV_RATE_LIMIT_PER_MIN = 2  # 2 requests per minute
            mock_settings.AV_RATE_LIMIT_PER_DAY = 10  # 10 requests per day
            mock_settings.AKSHARE_EASTMONEY_RATE_LIMIT = 3  # 3 requests per minute
            mock_settings.AKSHARE_DEFAULT_RATE_LIMIT = 1  # 1 request per minute
            
            await manager._ensure_initialized()
            
            # Test Alpha Vantage rate limiting
            start_time = time.time()
            
            # First two requests should succeed quickly
            await manager.acquire('alphavantage')
            await manager.acquire('alphavantage')
            
            # Third request should be blocked
            third_start = time.time()
            await manager.acquire('alphavantage')
            third_duration = time.time() - third_start
            
            # Should take at least some time due to rate limiting
            assert third_duration > 0.1
            
            # Test daily limit
            await manager.acquire_daily('alphavantage')
            
            # Test AkShare rate limiting
            await manager.acquire('akshare', 'eastmoney')
            await manager.acquire('akshare', 'eastmoney')
            await manager.acquire('akshare', 'eastmoney')
            
            # Fourth request should be blocked
            fourth_start = time.time()
            await manager.acquire('akshare', 'eastmoney')
            fourth_duration = time.time() - fourth_start
            
            assert fourth_duration > 0.1
    
    @pytest.mark.asyncio
    async def test_concurrent_rate_limiting(self):
        """Test concurrent rate limiting behavior."""
        manager = RateLimiterManager()
        
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            mock_settings.AV_RATE_LIMIT_PER_MIN = 3  # 3 requests per minute
            mock_settings.AKSHARE_EASTMONEY_RATE_LIMIT = 2  # 2 requests per minute
            
            await manager._ensure_initialized()
            
            async def make_request(request_id: int):
                start_time = time.time()
                await manager.acquire('alphavantage')
                await manager.acquire('akshare', 'eastmoney')
                duration = time.time() - start_time
                return request_id, duration
            
            # Start 5 concurrent requests
            tasks = [make_request(i) for i in range(5)]
            start_time = time.time()
            
            results = await asyncio.gather(*tasks)
            total_time = time.time() - start_time
            
            # All requests should complete
            assert len(results) == 5
            
            # Some requests should take longer due to rate limiting
            durations = [duration for _, duration in results]
            assert max(durations) > min(durations)  # Some variation due to rate limiting


class TestRateLimiterErrorHandling:
    """Test rate limiter error handling."""
    
    @pytest.mark.asyncio
    async def test_limiter_not_found(self):
        """Test handling when limiter is not found."""
        manager = RateLimiterManager()
        
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            
            await manager._ensure_initialized()
            
            # Try to acquire from non-existent limiter
            await manager.acquire('nonexistent')
            # Should not raise exception, just log warning
    
    @pytest.mark.asyncio
    async def test_limiter_initialization_error(self):
        """Test handling of limiter initialization errors."""
        manager = RateLimiterManager()
        
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            mock_settings.AV_RATE_LIMIT_PER_MIN = 5
            
            # Mock AsyncLimiter to raise exception
            with patch('ak_unified.rate_limiter.AsyncLimiter') as mock_limiter:
                mock_limiter.side_effect = Exception("Limiter creation failed")
                
                # Should handle initialization error gracefully
                await manager._ensure_initialized()
                
                assert manager._initialized is True
                assert len(manager._limiters) == 0


class TestRateLimiterConfiguration:
    """Test rate limiter configuration options."""
    
    @pytest.mark.asyncio
    async def test_custom_rate_limits(self):
        """Test custom rate limit configuration."""
        manager = RateLimiterManager()
        
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            mock_settings.AV_RATE_LIMIT_PER_MIN = 10  # Custom: 10 per minute
            mock_settings.AV_RATE_LIMIT_PER_DAY = 1000  # Custom: 1000 per day
            mock_settings.AKSHARE_EASTMONEY_RATE_LIMIT = 120  # Custom: 120 per minute
            mock_settings.AKSHARE_SINA_RATE_LIMIT = 200  # Custom: 200 per minute
            
            await manager._ensure_initialized()
            
            # Check that custom limits are applied
            alphavantage_limiter = manager._limiters['alphavantage']
            assert alphavantage_limiter.max_rate == 10
            assert alphavantage_limiter.time_period == 60.0
            
            daily_limiter = manager._daily_limiters['alphavantage']
            assert daily_limiter.max_rate == 1000
            assert daily_limiter.time_period == 86400.0
            
            eastmoney_limiter = manager._limiters['akshare_eastmoney']
            assert eastmoney_limiter.max_rate == 120
            assert eastmoney_limiter.time_period == 60.0
    
    @pytest.mark.asyncio
    async def test_vendor_specific_limits(self):
        """Test vendor-specific rate limits."""
        manager = RateLimiterManager()
        
        with patch('ak_unified.rate_limiter.settings') as mock_settings:
            mock_settings.RATE_LIMIT_ENABLED = True
            mock_settings.AKSHARE_EASTMONEY_RATE_LIMIT = 60
            mock_settings.AKSHARE_SINA_RATE_LIMIT = 100
            mock_settings.AKSHARE_TENCENT_RATE_LIMIT = 80
            mock_settings.AKSHARE_THS_RATE_LIMIT = 30
            mock_settings.AKSHARE_TDX_RATE_LIMIT = 50
            mock_settings.AKSHARE_BAIDU_RATE_LIMIT = 40
            mock_settings.AKSHARE_NETEASE_RATE_LIMIT = 60
            mock_settings.AKSHARE_HEXUN_RATE_LIMIT = 30
            mock_settings.AKSHARE_CSINDEX_RATE_LIMIT = 20
            mock_settings.AKSHARE_JISILU_RATE_LIMIT = 10
            mock_settings.AKSHARE_DEFAULT_RATE_LIMIT = 30
            
            await manager._ensure_initialized()
            
            # Check all vendor limiters are created
            expected_vendors = [
                'akshare_eastmoney', 'akshare_sina', 'akshare_tencent',
                'akshare_ths', 'akshare_tdx', 'akshare_baidu',
                'akshare_netease', 'akshare_hexun', 'akshare_csindex',
                'akshare_jisilu', 'akshare_default'
            ]
            
            for vendor in expected_vendors:
                assert vendor in manager._limiters
                
            # Check specific limits
            assert manager._limiters['akshare_eastmoney'].max_rate == 60
            assert manager._limiters['akshare_sina'].max_rate == 100
            assert manager._limiters['akshare_ths'].max_rate == 30
            assert manager._limiters['akshare_jisilu'].max_rate == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])