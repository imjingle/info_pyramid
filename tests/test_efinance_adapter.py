"""
Comprehensive tests for efinance adapter.
Tests efinance integration functionality for Chinese market data.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from ak_unified.adapters.efinance_adapter import (
    _import_efinance,
    call_efinance,
    EfinanceAdapterError
)


class TestEfinanceImport:
    """Test efinance module import functionality."""
    
    def test_import_efinance_success(self):
        """Test successful efinance import."""
        with patch('builtins.__import__') as mock_import:
            mock_ef = MagicMock()
            mock_import.return_value = mock_ef
            
            result = _import_efinance()
            
            assert result == mock_ef
            mock_import.assert_called_once_with('efinance')
    
    def test_import_efinance_failure(self):
        """Test efinance import failure."""
        with patch('builtins.__import__') as mock_import:
            mock_import.side_effect = ImportError("efinance not found")
            
            with pytest.raises(EfinanceAdapterError) as exc_info:
                _import_efinance()
            
            assert "Failed to import efinance" in str(exc_info.value)


class TestEfinanceDataFetching:
    """Test efinance data fetching functionality."""
    
    @patch('ak_unified.adapters.efinance_adapter._import_efinance')
    def test_call_efinance_stock_hist(self, mock_import_efinance):
        """Test stock history data fetching."""
        mock_ef = MagicMock()
        mock_import_efinance.return_value = mock_ef
        
        # Mock stock history response
        mock_df = pd.DataFrame({
            '日期': ['2024-01-01', '2024-01-02'],
            '开盘': [10.0, 10.1],
            '收盘': [10.1, 10.2],
            '最高': [10.2, 10.3],
            '最低': [9.9, 10.0],
            '成交量': [1000000, 1100000],
            '成交额': [10000000, 11000000]
        })
        mock_ef.stock.get_quote_history.return_value = mock_df
        
        result = call_efinance(
            'securities.equity.cn.ohlcv_daily.efinance',
            {'symbol': '600000', 'start': '2024-01-01', 'end': '2024-01-02'}
        )
        
        assert result[0] == 'efinance.stock.get_quote_history'
        assert isinstance(result[1], pd.DataFrame)
        assert len(result[1]) == 2
        assert 'symbol' in result[1].columns
        assert result[1]['symbol'].iloc[0] == '600000'
        
        # Verify efinance function was called correctly
        mock_ef.stock.get_quote_history.assert_called_once_with(
            '600000', beg='2024-01-01', end='2024-01-02'
        )
    
    @patch('ak_unified.adapters.efinance_adapter._import_efinance')
    def test_call_efinance_stock_quote(self, mock_import_efinance):
        """Test stock quote data fetching."""
        mock_ef = MagicMock()
        mock_import_efinance.return_value = mock_ef
        
        # Mock stock quote response
        mock_df = pd.DataFrame({
            '代码': ['600000', '600001'],
            '名称': ['浦发银行', '邯郸钢铁'],
            '最新价': [10.1, 20.2],
            '涨跌幅': [1.0, -0.5],
            '成交量': [1000000, 2000000]
        })
        mock_ef.stock.get_realtime_quotes.return_value = mock_df
        
        result = call_efinance(
            'securities.equity.cn.quote.efinance',
            {'symbols': ['600000', '600001']}
        )
        
        assert result[0] == 'efinance.stock.get_realtime_quotes'
        assert isinstance(result[1], pd.DataFrame)
        assert len(result[1]) == 2
        assert 'symbol' in result[1].columns
    
    @patch('ak_unified.adapters.efinance_adapter._import_efinance')
    def test_call_efinance_fund_hist(self, mock_import_efinance):
        """Test fund history data fetching."""
        mock_ef = MagicMock()
        mock_import_efinance.return_value = mock_ef
        
        # Mock fund history response
        mock_df = pd.DataFrame({
            '日期': ['2024-01-01', '2024-01-02'],
            '净值': [1.0, 1.01],
            '累计净值': [1.5, 1.51],
            '日增长率': [0.0, 1.0]
        })
        mock_ef.fund.get_quote_history.return_value = mock_df
        
        result = call_efinance(
            'securities.fund.cn.ohlcv_daily.efinance',
            {'symbol': '000001', 'start': '2024-01-01', 'end': '2024-01-02'}
        )
        
        assert result[0] == 'efinance.fund.get_quote_history'
        assert isinstance(result[1], pd.DataFrame)
        assert len(result[1]) == 2
        assert 'symbol' in result[1].columns


class TestEfinanceErrorHandling:
    """Test efinance error handling scenarios."""
    
    def test_efinance_adapter_error_inheritance(self):
        """Test efinance adapter error inheritance."""
        error = EfinanceAdapterError("Test error")
        assert isinstance(error, RuntimeError)
        assert str(error) == "Test error"
    
    @patch('ak_unified.adapters.efinance_adapter._import_efinance')
    def test_call_efinance_function_error(self, mock_import_efinance):
        """Test efinance function error handling."""
        mock_ef = MagicMock()
        mock_import_efinance.return_value = mock_ef
        
        mock_ef.stock.get_quote_history.side_effect = Exception("Network error")
        
        with pytest.raises(EfinanceAdapterError) as exc_info:
            call_efinance(
                'securities.equity.cn.ohlcv_daily.efinance',
                {'symbol': '600000'}
            )
        
        assert "Network error" in str(exc_info.value)
    
    @patch('ak_unified.adapters.efinance_adapter._import_efinance')
    def test_call_efinance_empty_data(self, mock_import_efinance):
        """Test handling of empty data returns."""
        mock_ef = MagicMock()
        mock_import_efinance.return_value = mock_ef
        
        mock_ef.stock.get_quote_history.return_value = pd.DataFrame()
        
        result = call_efinance(
            'securities.equity.cn.ohlcv_daily.efinance',
            {'symbol': 'INVALID'}
        )
        
        assert result[0] == 'efinance.stock.get_quote_history'
        assert isinstance(result[1], pd.DataFrame)
        assert result[1].empty


class TestEfinanceDataTransformation:
    """Test efinance data transformation functionality."""
    
    @patch('ak_unified.adapters.efinance_adapter._import_efinance')
    def test_data_column_mapping(self, mock_import_efinance):
        """Test data column mapping and transformation."""
        mock_ef = MagicMock()
        mock_import_efinance.return_value = mock_ef
        
        # Mock data with Chinese column names
        mock_df = pd.DataFrame({
            '日期': ['2024-01-01', '2024-01-02'],
            '开盘': [10.0, 10.1],
            '收盘': [10.1, 10.2],
            '最高': [10.2, 10.3],
            '最低': [9.9, 10.0],
            '成交量': [1000000, 1100000],
            '成交额': [10000000, 11000000]
        })
        mock_ef.stock.get_quote_history.return_value = mock_df
        
        result = call_efinance(
            'securities.equity.cn.ohlcv_daily.efinance',
            {'symbol': '600000', 'start': '2024-01-01', 'end': '2024-01-02'}
        )
        
        df = result[1]
        
        # Check that Chinese columns are preserved
        assert '日期' in df.columns
        assert '开盘' in df.columns
        assert '收盘' in df.columns
        assert '最高' in df.columns
        assert '最低' in df.columns
        assert '成交量' in df.columns
        assert '成交额' in df.columns
        
        # Check that symbol column was added
        assert 'symbol' in df.columns
        assert df['symbol'].iloc[0] == '600000'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])