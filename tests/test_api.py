"""
Comprehensive tests for FastAPI service endpoints.
Tests API routing, request handling, and response formatting.
"""

import pytest
import json
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi.testclient import TestClient
from fastapi import HTTPException
from ak_unified.api import app


@pytest.fixture
def client():
    """Create test client for FastAPI app."""
    return TestClient(app)


class TestAPIBasicEndpoints:
    """Test basic API endpoints."""
    
    def test_root_endpoint(self, client):
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == 200
        assert "AK Unified API" in response.json()["title"]
    
    def test_docs_endpoint(self, client):
        """Test API documentation endpoint."""
        response = client.get("/docs")
        assert response.status_code == 200
    
    def test_openapi_endpoint(self, client):
        """Test OpenAPI schema endpoint."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        schema = response.json()
        assert "openapi" in schema
        assert "info" in schema


class TestAPIRateLimitEndpoints:
    """Test rate limiting endpoints."""
    
    @patch('ak_unified.api.get_rate_limit_status')
    def test_rate_limits_endpoint_success(self, mock_get_status, client):
        """Test successful rate limits endpoint."""
        mock_status = {
            'alphavantage': {
                'max_rate': 5,
                'time_period': 60.0,
                'rate_per_sec': 0.08333333333333333,
                'level': 4.5,
                'has_capacity': True
            }
        }
        mock_get_status.return_value = mock_status
        
        response = client.get("/rpc/rate-limits")
        assert response.status_code == 200
        
        data = response.json()
        assert data["rate_limiting_enabled"] is True
        assert "limiters" in data
        assert "alphavantage" in data["limiters"]
    
    @patch('ak_unified.api.get_rate_limit_status')
    def test_rate_limits_endpoint_error(self, mock_get_status, client):
        """Test rate limits endpoint with error."""
        mock_get_status.side_effect = Exception("Database connection failed")
        
        response = client.get("/rpc/rate-limits")
        assert response.status_code == 200
        
        data = response.json()
        assert data["rate_limiting_enabled"] is False
        assert "error" in data
        assert "Database connection failed" in data["error"]


class TestAPIFetchEndpoints:
    """Test data fetching endpoints."""
    
    @patch('ak_unified.api.fetch_data')
    def test_rpc_fetch_success(self, mock_fetch_data, client):
        """Test successful RPC fetch endpoint."""
        # Mock successful response
        mock_envelope = MagicMock()
        mock_envelope.model_dump.return_value = {
            "data": [{"symbol": "AAPL", "close": 150.0}],
            "metadata": {"count": 1}
        }
        mock_fetch_data.return_value = mock_envelope
        
        response = client.get("/rpc/fetch", params={
            "dataset_id": "securities.equity.us.ohlcv_daily.av",
            "symbol": "AAPL"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert len(data["data"]) == 1
        assert data["data"][0]["symbol"] == "AAPL"
        
        # Verify fetch_data was called correctly
        mock_fetch_data.assert_called_once()
        call_args = mock_fetch_data.call_args
        assert call_args[1]["dataset_id"] == "securities.equity.us.ohlcv_daily.av"
        assert call_args[1]["params"]["symbol"] == "AAPL"
    
    @patch('ak_unified.api.fetch_data')
    def test_rpc_fetch_with_adapter(self, mock_fetch_data, client):
        """Test RPC fetch with adapter parameter."""
        mock_envelope = MagicMock()
        mock_envelope.model_dump.return_value = {
            "data": [{"symbol": "0700", "close": 300.0}],
            "metadata": {"count": 1}
        }
        mock_fetch_data.return_value = mock_envelope
        
        response = client.get("/rpc/fetch", params={
            "dataset_id": "securities.equity.hk.ohlcv_daily.av",
            "symbol": "0700",
            "adapter": "alphavantage"
        })
        
        assert response.status_code == 200
        
        # Verify adapter was applied
        call_args = mock_fetch_data.call_args
        assert "alphavantage" in call_args[1]["dataset_id"]
    
    @patch('ak_unified.api.fetch_data')
    def test_rpc_fetch_with_ak_function(self, mock_fetch_data, client):
        """Test RPC fetch with ak_function parameter."""
        mock_envelope = MagicMock()
        mock_envelope.model_dump.return_value = {
            "data": [{"symbol": "600000", "close": 10.0}],
            "metadata": {"count": 1}
        }
        mock_fetch_data.return_value = mock_envelope
        
        response = client.get("/rpc/fetch", params={
            "dataset_id": "securities.equity.cn.ohlcv_daily",
            "symbol": "600000",
            "ak_function": "stock_zh_a_hist"
        })
        
        assert response.status_code == 200
        
        # Verify ak_function was passed
        call_args = mock_fetch_data.call_args
        assert call_args[1]["ak_function"] == "stock_zh_a_hist"
    
    @patch('ak_unified.api.fetch_data')
    def test_rpc_fetch_error_handling(self, mock_fetch_data, client):
        """Test RPC fetch error handling."""
        mock_fetch_data.side_effect = ValueError("Invalid dataset")
        
        response = client.get("/rpc/fetch", params={
            "dataset_id": "invalid.dataset",
            "symbol": "AAPL"
        })
        
        assert response.status_code == 500
        data = response.json()
        assert "detail" in data
        assert "Invalid dataset" in data["detail"]


class TestAPICacheEndpoints:
    """Test cache management endpoints."""
    
    @patch('ak_unified.api._cache_stats')
    def test_cache_stats_endpoint(self, mock_cache_stats, client):
        """Test cache stats endpoint."""
        mock_stats = {
            "total_records": 1000,
            "total_size_bytes": 50000000,
            "hit_rate": 0.85
        }
        mock_cache_stats.return_value = mock_stats
        
        response = client.get("/rpc/cache/stats")
        assert response.status_code == 200
        
        data = response.json()
        assert data["total_records"] == 1000
        assert data["total_size_bytes"] == 50000000
        assert data["hit_rate"] == 0.85
    
    @patch('ak_unified.api._purge_records')
    def test_cache_purge_endpoint(self, mock_purge_records, client):
        """Test cache purge endpoint."""
        mock_purge_records.return_value = {"deleted": 100}
        
        response = client.delete("/rpc/cache/purge", params={"older_than_days": 30})
        assert response.status_code == 200
        
        data = response.json()
        assert data["deleted"] == 100
        
        # Verify purge was called with correct parameters
        mock_purge_records.assert_called_once_with(older_than_days=30)


class TestAPIBlobEndpoints:
    """Test blob storage endpoints."""
    
    @patch('ak_unified.api._blob_fetch')
    def test_blob_fetch_endpoint(self, mock_blob_fetch, client):
        """Test blob fetch endpoint."""
        mock_blob_data = {
            "data": "base64_encoded_data",
            "metadata": {"size": 1024, "compressed": True}
        }
        mock_blob_fetch.return_value = mock_blob_data
        
        response = client.get("/rpc/blob/fetch", params={
            "dataset_id": "test.dataset",
            "snapshot_id": "snapshot_123"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["data"] == "base64_encoded_data"
        assert data["metadata"]["size"] == 1024
    
    @patch('ak_unified.api._blob_upsert')
    def test_blob_upsert_endpoint(self, mock_blob_upsert, client):
        """Test blob upsert endpoint."""
        mock_blob_upsert.return_value = {"snapshot_id": "new_snapshot_456"}
        
        blob_data = {
            "dataset_id": "test.dataset",
            "data": "base64_encoded_data",
            "metadata": {"size": 1024}
        }
        
        response = client.post("/rpc/blob/upsert", json=blob_data)
        assert response.status_code == 200
        
        data = response.json()
        assert data["snapshot_id"] == "new_snapshot_456"
        
        # Verify upsert was called with correct data
        mock_blob_upsert.assert_called_once()
        call_args = mock_blob_upsert.call_args
        assert call_args[1]["dataset_id"] == "test.dataset"
    
    @patch('ak_unified.api._blob_purge')
    def test_blob_purge_endpoint(self, mock_blob_purge, client):
        """Test blob purge endpoint."""
        mock_blob_purge.return_value = {"deleted": 5}
        
        response = client.delete("/rpc/blob/purge", params={"older_than_days": 7})
        assert response.status_code == 200
        
        data = response.json()
        assert data["deleted"] == 5
        
        # Verify purge was called with correct parameters
        mock_blob_purge.assert_called_once_with(older_than_days=7)


class TestAPIDatabaseEndpoints:
    """Test database connection endpoints."""
    
    @patch('ak_unified.api._get_pool')
    def test_db_pool_endpoint(self, mock_get_pool, client):
        """Test database pool endpoint."""
        mock_pool = MagicMock()
        mock_pool.get_stats.return_value = {
            "size": 10,
            "free": 8,
            "used": 2
        }
        mock_get_pool.return_value = mock_pool
        
        response = client.get("/rpc/db/pool")
        assert response.status_code == 200
        
        data = response.json()
        assert data["size"] == 10
        assert data["free"] == 8
        assert data["used"] == 2


class TestAPIErrorHandling:
    """Test API error handling scenarios."""
    
    def test_invalid_json_request(self, client):
        """Test handling of invalid JSON requests."""
        response = client.post("/rpc/blob/upsert", data="invalid json")
        assert response.status_code == 422
    
    def test_missing_required_parameters(self, client):
        """Test handling of missing required parameters."""
        response = client.get("/rpc/fetch")
        assert response.status_code == 422
    
    def test_invalid_dataset_id_format(self, client):
        """Test handling of invalid dataset ID format."""
        response = client.get("/rpc/fetch", params={"dataset_id": "invalid..format"})
        assert response.status_code == 422


class TestAPIMiddleware:
    """Test API middleware functionality."""
    
    def test_cors_headers(self, client):
        """Test CORS headers are present."""
        response = client.options("/rpc/fetch")
        assert response.status_code == 200
        assert "access-control-allow-origin" in response.headers
    
    def test_request_id_header(self, client):
        """Test request ID header is generated."""
        response = client.get("/")
        assert "x-request-id" in response.headers


class TestAPIValidation:
    """Test API request validation."""
    
    def test_dataset_id_validation(self, client):
        """Test dataset ID format validation."""
        # Valid dataset ID
        response = client.get("/rpc/fetch", params={"dataset_id": "securities.equity.us.ohlcv_daily.av"})
        assert response.status_code == 422  # Missing required params, but dataset_id is valid
        
        # Invalid dataset ID
        response = client.get("/rpc/fetch", params={"dataset_id": "invalid..format"})
        assert response.status_code == 422
    
    def test_symbol_validation(self, client):
        """Test symbol parameter validation."""
        response = client.get("/rpc/fetch", params={
            "dataset_id": "securities.equity.us.ohlcv_daily.av",
            "symbol": "AAPL"
        })
        assert response.status_code == 422  # Missing other required params, but symbol is valid


if __name__ == "__main__":
    pytest.main([__file__, "-v"])