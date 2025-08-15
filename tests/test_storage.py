"""
Comprehensive tests for PostgreSQL storage layer.
Tests database connections, queries, and data operations.
"""

import pytest
import asyncio
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timedelta
from ak_unified.storage import (
    get_pool,
    fetch_records,
    upsert_records,
    upsert_blob_snapshot,
    fetch_blob_snapshot,
    fetch_blob_range,
    cache_stats,
    purge_records,
    purge_blob
)


class TestDatabaseConnection:
    """Test database connection functionality."""
    
    @patch('ak_unified.storage.asyncpg.create_pool')
    @pytest.mark.asyncio
    async def test_get_pool_success(self, mock_create_pool):
        """Test successful database pool creation."""
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool
        
        pool = await get_pool()
        
        assert pool == mock_pool
        mock_create_pool.assert_called_once()
    
    @patch('ak_unified.storage.asyncpg.create_pool')
    @pytest.mark.asyncio
    async def test_get_pool_connection_error(self, mock_create_pool):
        """Test database pool creation error handling."""
        mock_create_pool.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception) as exc_info:
            await get_pool()
        
        assert "Connection failed" in str(exc_info.value)
    
    @patch('ak_unified.storage.asyncpg.create_pool')
    @pytest.mark.asyncio
    async def test_get_pool_environment_config(self, mock_create_pool):
        """Test database pool creation with environment configuration."""
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool
        
        with patch.dict('os.environ', {
            'AKU_DB_DSN': 'postgresql://user:pass@localhost:5432/testdb',
            'AKU_DB_MIN_SIZE': '5',
            'AKU_DB_MAX_SIZE': '20'
        }):
            pool = await get_pool()
            
            assert pool == mock_pool
            # Verify environment variables were used
            call_args = mock_create_pool.call_args
            assert 'postgresql://user:pass@localhost:5432/testdb' in str(call_args)


class TestRecordOperations:
    """Test record CRUD operations."""
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_fetch_records_success(self, mock_get_pool):
        """Test successful record fetching."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        # Mock query result
        mock_rows = [
            {'id': 1, 'symbol': 'AAPL', 'close': 150.0, 'date': '2024-01-01'},
            {'id': 2, 'symbol': 'AAPL', 'close': 151.0, 'date': '2024-01-02'}
        ]
        mock_pool.fetch.return_value = mock_rows
        
        result = await fetch_records(
            'securities.equity.us.ohlcv_daily.av',
            {'symbol': 'AAPL'},
            limit=10
        )
        
        assert len(result) == 2
        assert result[0]['symbol'] == 'AAPL'
        assert result[1]['close'] == 151.0
        
        # Verify query was executed
        mock_pool.fetch.assert_called_once()
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_fetch_records_empty_result(self, mock_get_pool):
        """Test record fetching with empty result."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.fetch.return_value = []
        
        result = await fetch_records(
            'securities.equity.us.ohlcv_daily.av',
            {'symbol': 'INVALID'}
        )
        
        assert result == []
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_upsert_records_success(self, mock_get_pool):
        """Test successful record upserting."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.executemany.return_value = "INSERT 0 2"
        
        records = [
            {'symbol': 'AAPL', 'close': 150.0, 'date': '2024-01-01'},
            {'symbol': 'MSFT', 'close': 300.0, 'date': '2024-01-01'}
        ]
        
        result = await upsert_records(
            'securities.equity.us.ohlcv_daily.av',
            records
        )
        
        assert result == "INSERT 0 2"
        
        # Verify executemany was called
        mock_pool.executemany.assert_called_once()
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_upsert_records_error(self, mock_get_pool):
        """Test record upserting error handling."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.executemany.side_effect = Exception("Database error")
        
        records = [{'symbol': 'AAPL', 'close': 150.0}]
        
        with pytest.raises(Exception) as exc_info:
            await upsert_records(
                'securities.equity.us.ohlcv_daily.av',
                records
            )
        
        assert "Database error" in str(exc_info.value)


class TestBlobOperations:
    """Test blob storage operations."""
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_upsert_blob_snapshot_success(self, mock_get_pool):
        """Test successful blob snapshot upserting."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.fetchval.return_value = "snapshot_123"
        
        blob_data = {
            'dataset_id': 'test.dataset',
            'data': b'binary_data_here',
            'metadata': {'size': 1024, 'compressed': True}
        }
        
        result = await upsert_blob_snapshot(**blob_data)
        
        assert result == "snapshot_123"
        
        # Verify execute was called
        mock_pool.execute.assert_called_once()
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_fetch_blob_snapshot_success(self, mock_get_pool):
        """Test successful blob snapshot fetching."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        # Mock blob data
        mock_row = {
            'data': b'binary_data_here',
            'metadata': {'size': 1024, 'compressed': True},
            'created_at': datetime.now()
        }
        mock_pool.fetchrow.return_value = mock_row
        
        result = await fetch_blob_snapshot(
            'test.dataset',
            'snapshot_123'
        )
        
        assert result['data'] == b'binary_data_here'
        assert result['metadata']['size'] == 1024
        assert result['metadata']['compressed'] is True
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_fetch_blob_snapshot_not_found(self, mock_get_pool):
        """Test blob snapshot fetching when not found."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.fetchrow.return_value = None
        
        result = await fetch_blob_snapshot(
            'test.dataset',
            'nonexistent_snapshot'
        )
        
        assert result is None
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_fetch_blob_range_success(self, mock_get_pool):
        """Test successful blob range fetching."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        # Mock range data
        mock_rows = [
            {
                'snapshot_id': 'snapshot_1',
                'data': b'data_1',
                'metadata': {'size': 512},
                'created_at': datetime.now() - timedelta(days=1)
            },
            {
                'snapshot_id': 'snapshot_2',
                'data': b'data_2',
                'metadata': {'size': 512},
                'created_at': datetime.now()
            }
        ]
        mock_pool.fetch.return_value = mock_rows
        
        result = await fetch_blob_range(
            'test.dataset',
            start_date='2024-01-01',
            end_date='2024-01-02'
        )
        
        assert len(result) == 2
        assert result[0]['snapshot_id'] == 'snapshot_1'
        assert result[1]['snapshot_id'] == 'snapshot_2'


class TestCacheOperations:
    """Test cache management operations."""
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_cache_stats_success(self, mock_get_pool):
        """Test successful cache statistics retrieval."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        # Mock stats data
        mock_stats = [
            {'total_records': 1000, 'total_size_bytes': 50000000}
        ]
        mock_pool.fetch.return_value = mock_stats
        
        result = await cache_stats()
        
        assert result['total_records'] == 1000
        assert result['total_size_bytes'] == 50000000
        assert 'hit_rate' in result  # Calculated field
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_purge_records_success(self, mock_get_pool):
        """Test successful record purging."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.fetchval.return_value = 100  # Number of deleted records
        
        result = await purge_records(older_than_days=30)
        
        assert result['deleted'] == 100
        
        # Verify execute was called
        mock_pool.execute.assert_called_once()
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_purge_blob_success(self, mock_get_pool):
        """Test successful blob purging."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.fetchval.return_value = 5  # Number of deleted blobs
        
        result = await purge_blob(older_than_days=7)
        
        assert result['deleted'] == 5
        
        # Verify execute was called
        mock_pool.execute.assert_called_once()


class TestDataValidation:
    """Test data validation and sanitization."""
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_fetch_records_sql_injection_protection(self, mock_get_pool):
        """Test SQL injection protection in record fetching."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.fetch.return_value = []
        
        # Test with potentially malicious input
        malicious_params = {
            'symbol': "'; DROP TABLE records; --"
        }
        
        await fetch_records(
            'securities.equity.us.ohlcv_daily.av',
            malicious_params
        )
        
        # Verify the query was executed with parameterized query
        mock_pool.fetch.assert_called_once()
        call_args = mock_pool.fetch.call_args
        
        # The query should use parameterized queries, not string concatenation
        query = call_args[0][0]
        assert 'DROP TABLE' not in query
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_upsert_records_data_sanitization(self, mock_get_pool):
        """Test data sanitization in record upserting."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.executemany.return_value = "INSERT 0 1"
        
        # Test with various data types and edge cases
        records = [
            {
                'symbol': 'AAPL',
                'close': 150.0,
                'date': '2024-01-01',
                'volume': 1000000,
                'null_value': None,
                'empty_string': '',
                'special_chars': 'test\'"\\'
            }
        ]
        
        await upsert_records(
            'securities.equity.us.ohlcv_daily.av',
            records
        )
        
        # Verify executemany was called
        mock_pool.executemany.assert_called_once()
        call_args = mock_pool.executemany.call_args
        
        # Check that the query and data are properly formatted
        query = call_args[0][0]
        data = call_args[0][1]
        
        assert 'INSERT INTO' in query
        assert len(data) == 1
        assert data[0]['symbol'] == 'AAPL'


class TestErrorHandling:
    """Test error handling scenarios."""
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_connection_pool_error(self, mock_get_pool):
        """Test handling of connection pool errors."""
        mock_get_pool.side_effect = Exception("Pool connection failed")
        
        with pytest.raises(Exception) as exc_info:
            await fetch_records('test.dataset', {})
        
        assert "Pool connection failed" in str(exc_info.value)
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_transaction_rollback(self, mock_get_pool):
        """Test transaction rollback on error."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        # Simulate transaction error
        mock_pool.execute.side_effect = Exception("Transaction failed")
        
        with pytest.raises(Exception):
            await upsert_records('test.dataset', [{'test': 'data'}])
        
        # Verify rollback was called
        mock_pool.rollback.assert_called_once()


class TestPerformanceOptimization:
    """Test performance optimization features."""
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_batch_upsert_optimization(self, mock_get_pool):
        """Test batch upsert optimization."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.executemany.return_value = "INSERT 0 1000"
        
        # Create large batch of records
        records = [
            {'symbol': f'STOCK_{i}', 'close': 100.0 + i, 'date': '2024-01-01'}
            for i in range(1000)
        ]
        
        start_time = datetime.now()
        result = await upsert_records('test.dataset', records)
        end_time = datetime.now()
        
        assert result == "INSERT 0 1000"
        
        # Verify executemany was used (not multiple execute calls)
        mock_pool.executemany.assert_called_once()
        assert mock_pool.execute.call_count == 0
        
        # Performance check: should complete within reasonable time
        duration = (end_time - start_time).total_seconds()
        assert duration < 1.0  # Should complete in less than 1 second
    
    @patch('ak_unified.storage.get_pool')
    @pytest.mark.asyncio
    async def test_query_optimization(self, mock_get_pool):
        """Test query optimization features."""
        mock_pool = AsyncMock()
        mock_get_pool.return_value = mock_pool
        
        mock_pool.fetch.return_value = []
        
        # Test with limit and offset for pagination
        await fetch_records(
            'test.dataset',
            {'symbol': 'AAPL'},
            limit=100,
            offset=200
        )
        
        # Verify the query includes LIMIT and OFFSET
        mock_pool.fetch.assert_called_once()
        call_args = mock_pool.fetch.call_args
        query = call_args[0][0]
        
        assert 'LIMIT' in query
        assert 'OFFSET' in query


if __name__ == "__main__":
    pytest.main([__file__, "-v"])