"""
Comprehensive tests for QMT adapter.
Tests QMT/ThinkTrader integration functionality.
"""

import pytest
import pandas as pd
import platform
from unittest.mock import patch, MagicMock, mock_open
from ak_unified.adapters.qmt_adapter import (
    is_windows,
    _ensure_windows,
    _import_qmt_module,
    _get_mapping,
    get_qmt_mapping,
    test_qmt_import,
    _to_dataframe,
    QmtAdapterError
)


class TestQMTPlatformDetection:
    """Test QMT platform detection functionality."""
    
    def test_is_windows(self):
        """Test Windows platform detection."""
        # Mock platform.system
        with patch('platform.system') as mock_platform:
            mock_platform.return_value = 'Windows'
            assert is_windows() is True
            
            mock_platform.return_value = 'Linux'
            assert is_windows() is False
            
            mock_platform.return_value = 'Darwin'
            assert is_windows() is False
    
    def test_ensure_windows_success(self):
        """Test Windows requirement check on Windows."""
        with patch('ak_unified.adapters.qmt_adapter.is_windows') as mock_is_windows:
            mock_is_windows.return_value = True
            # Should not raise exception
            _ensure_windows()
    
    def test_ensure_windows_failure(self):
        """Test Windows requirement check on non-Windows."""
        with patch('ak_unified.adapters.qmt_adapter.is_windows') as mock_is_windows:
            mock_is_windows.return_value = False
            with pytest.raises(QmtAdapterError) as exc_info:
                _ensure_windows()
            assert "QMT adapter is Windows-only" in str(exc_info.value)


class TestQMTModuleImport:
    """Test QMT module import functionality."""
    
    def test_import_qmt_module_with_env_var(self):
        """Test QMT module import with environment variable."""
        with patch.dict('os.environ', {'AKU_QMT_PYMOD': 'custom_qmt'}):
            with patch('builtins.__import__') as mock_import:
                mock_module = MagicMock()
                mock_import.return_value = mock_module
                
                result = _import_qmt_module()
                
                assert result == mock_module
                mock_import.assert_called_once_with('custom_qmt')
    
    def test_import_qmt_module_fallback_candidates(self):
        """Test QMT module import with fallback candidates."""
        with patch.dict('os.environ', {}, clear=True):
            with patch('builtins.__import__') as mock_import:
                # First candidate fails
                mock_import.side_effect = [ImportError("Not found"), MagicMock()]
                
                result = _import_qmt_module()
                
                assert result is not None
                # Should have tried 'qmt' first, then 'qmt_native'
                assert mock_import.call_count == 2
    
    def test_import_qmt_module_all_fail(self):
        """Test QMT module import when all candidates fail."""
        with patch.dict('os.environ', {}, clear=True):
            with patch('builtins.__import__') as mock_import:
                mock_import.side_effect = ImportError("Not found")
                
                with pytest.raises(QmtAdapterError) as exc_info:
                    _import_qmt_module()
                
                assert "Failed to import QMT Python module" in str(exc_info.value)


class TestQMTMapping:
    """Test QMT function mapping functionality."""
    
    def test_get_mapping_default(self):
        """Test default QMT function mapping."""
        with patch.dict('os.environ', {}, clear=True):
            mapping = _get_mapping()
            
            # Check essential mappings exist
            assert 'ohlcv_daily' in mapping
            assert 'ohlcv_min' in mapping
            assert 'quote' in mapping
            assert 'calendar' in mapping
            assert 'adjust_factor' in mapping
            assert 'board_industry' in mapping
            assert 'board_concept' in mapping
            assert 'index_constituents' in mapping
            assert 'corporate_actions' in mapping
            
            # Check default values
            assert mapping['ohlcv_daily'] == 'get_kline_daily'
            assert mapping['ohlcv_min'] == 'get_kline_min'
            assert mapping['quote'] == 'get_realtime_quote'
    
    def test_get_mapping_with_config_file_json(self):
        """Test QMT mapping with JSON config file."""
        config_data = {
            'qmt': {
                'ohlcv_daily': 'custom_daily_func',
                'quote': 'custom_quote_func'
            }
        }
        
        with patch.dict('os.environ', {'AKU_QMT_CONFIG': '/path/to/config.json'}):
            with patch('builtins.open', mock_open(read_data=str(config_data).replace("'", '"'))):
                with patch('json.load') as mock_json_load:
                    mock_json_load.return_value = config_data
                    
                    mapping = _get_mapping()
                    
                    # Custom mappings should override defaults
                    assert mapping['ohlcv_daily'] == 'custom_daily_func'
                    assert mapping['quote'] == 'custom_quote_func'
                    # Other mappings should remain default
                    assert mapping['ohlcv_min'] == 'get_kline_min'
    
    def test_get_mapping_with_config_file_yaml(self):
        """Test QMT mapping with YAML config file."""
        config_data = {
            'qmt': {
                'ohlcv_min': 'custom_min_func',
                'calendar': 'custom_calendar_func'
            }
        }
        
        with patch.dict('os.environ', {'AKU_QMT_CONFIG': '/path/to/config.yml'}):
            with patch('builtins.open', mock_open(read_data=str(config_data))):
                with patch('yaml.safe_load') as mock_yaml_load:
                    mock_yaml_load.return_value = config_data
                    
                    mapping = _get_mapping()
                    
                    # Custom mappings should override defaults
                    assert mapping['ohlcv_min'] == 'custom_min_func'
                    assert mapping['calendar'] == 'custom_calendar_func'
                    # Other mappings should remain default
                    assert mapping['ohlcv_daily'] == 'get_kline_daily'
    
    def test_get_mapping_config_file_error(self):
        """Test QMT mapping with config file error."""
        with patch.dict('os.environ', {'AKU_QMT_CONFIG': '/path/to/config.json'}):
            with patch('builtins.open', side_effect=FileNotFoundError("File not found")):
                mapping = _get_mapping()
                
                # Should fall back to default mapping
                assert mapping['ohlcv_daily'] == 'get_kline_daily'
                assert mapping['quote'] == 'get_realtime_quote'
    
    def test_get_qmt_mapping(self):
        """Test get_qmt_mapping function."""
        with patch('ak_unified.adapters.qmt_adapter._get_mapping') as mock_get_mapping:
            expected_mapping = {'ohlcv_daily': 'get_kline_daily'}
            mock_get_mapping.return_value = expected_mapping
            
            result = get_qmt_mapping()
            
            assert result == expected_mapping
            mock_get_mapping.assert_called_once()


class TestQMTImportTest:
    """Test QMT import test functionality."""
    
    def test_test_qmt_import_success(self):
        """Test successful QMT import test."""
        with patch('ak_unified.adapters.qmt_adapter._ensure_windows') as mock_ensure_windows, \
             patch('ak_unified.adapters.qmt_adapter._import_qmt_module') as mock_import, \
             patch('ak_unified.adapters.qmt_adapter._get_mapping') as mock_get_mapping:
            
            mock_module = MagicMock()
            mock_module.__name__ = 'qmt'
            mock_import.return_value = mock_module
            
            mock_mapping = {'ohlcv_daily': 'get_kline_daily'}
            mock_get_mapping.return_value = mock_mapping
            
            result = test_qmt_import()
            
            assert result['ok'] is True
            assert result['module'] == 'qmt'
            assert result['mapping'] == mock_mapping
            assert 'available' in result
    
    def test_test_qmt_import_windows_error(self):
        """Test QMT import test with Windows requirement error."""
        with patch('ak_unified.adapters.qmt_adapter._ensure_windows') as mock_ensure_windows:
            mock_ensure_windows.side_effect = QmtAdapterError("Windows required")
            
            result = test_qmt_import()
            
            assert result['ok'] is False
            assert "Windows required" in result['error']
            assert result['is_windows'] is False
    
    def test_test_qmt_import_module_error(self):
        """Test QMT import test with module import error."""
        with patch('ak_unified.adapters.qmt_adapter._ensure_windows'), \
             patch('ak_unified.adapters.qmt_adapter._import_qmt_module') as mock_import:
            
            mock_import.side_effect = ImportError("Module not found")
            
            result = test_qmt_import()
            
            assert result['ok'] is False
            assert "Module not found" in result['error']


class TestQMTDataFrameConversion:
    """Test QMT data conversion functionality."""
    
    def test_to_dataframe_from_dataframe(self):
        """Test DataFrame conversion from DataFrame."""
        df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        result = _to_dataframe(df)
        
        assert isinstance(result, pd.DataFrame)
        assert result.equals(df)
    
    def test_to_dataframe_from_list(self):
        """Test DataFrame conversion from list."""
        data = [{'col1': 1, 'col2': 'a'}, {'col1': 2, 'col2': 'b'}]
        result = _to_dataframe(data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'col1' in result.columns
        assert 'col2' in result.columns
    
    def test_to_dataframe_from_dict(self):
        """Test DataFrame conversion from dict."""
        data = {'col1': 1, 'col2': 'a'}
        result = _to_dataframe(data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert 'col1' in result.columns
        assert 'col2' in result.columns
    
    def test_to_dataframe_from_empty(self):
        """Test DataFrame conversion from empty data."""
        result = _to_dataframe(None)
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestQMTErrorHandling:
    """Test QMT error handling scenarios."""
    
    def test_qmt_adapter_error_inheritance(self):
        """Test QMT adapter error inheritance."""
        error = QmtAdapterError("Test error")
        assert isinstance(error, RuntimeError)
        assert str(error) == "Test error"
    
    def test_config_file_invalid_format(self):
        """Test handling of invalid config file format."""
        with patch.dict('os.environ', {'AKU_QMT_CONFIG': '/path/to/config.json'}):
            with patch('builtins.open', mock_open(read_data="invalid json")):
                with patch('json.load', side_effect=ValueError("Invalid JSON")):
                    mapping = _get_mapping()
                    
                    # Should fall back to default mapping
                    assert mapping['ohlcv_daily'] == 'get_kline_daily'
    
    def test_config_file_missing_qmt_section(self):
        """Test handling of config file without qmt section."""
        config_data = {'other_section': {'key': 'value'}}
        
        with patch.dict('os.environ', {'AKU_QMT_CONFIG': '/path/to/config.json'}):
            with patch('builtins.open', mock_open(read_data=str(config_data).replace("'", '"'))):
                with patch('json.load') as mock_json_load:
                    mock_json_load.return_value = config_data
                    
                    mapping = _get_mapping()
                    
                    # Should fall back to default mapping
                    assert mapping['ohlcv_daily'] == 'get_kline_daily'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])