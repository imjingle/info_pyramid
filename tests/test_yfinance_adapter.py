"""
Comprehensive tests for YFinance adapter.
Tests yfinance integration functionality for US and HK markets.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from ak_unified.adapters.yfinance_adapter import (
    _import_yf,
    _norm_symbol_us,
    _norm_symbol_hk,
    _to_records,
    call_yfinance,
    YFAdapterError
)


class TestYFinanceImport:
    """Test YFinance module import functionality."""
    
    def test_import_yf_success(self):
        """Test successful yfinance import."""
        with patch('builtins.__import__') as mock_import:
            mock_yf = MagicMock()
            mock_import.return_value = mock_yf
            
            result = _import_yf()
            
            assert result == mock_yf
            mock_import.assert_called_once_with('yfinance')
    
    def test_import_yf_failure(self):
        """Test yfinance import failure."""
        with patch('builtins.__import__') as mock_import:
            mock_import.side_effect = ImportError("yfinance not found")
            
            with pytest.raises(YFAdapterError) as exc_info:
                _import_yf()
            
            assert "Failed to import yfinance" in str(exc_info.value)


class TestYFinanceSymbolNormalization:
    """Test symbol normalization functionality."""
    
    def test_norm_symbol_us(self):
        """Test US symbol normalization."""
        # Test basic normalization
        assert _norm_symbol_us('AAPL') == 'AAPL'
        assert _norm_symbol_us('  aapl  ') == 'AAPL'
        assert _norm_symbol_us('msft') == 'MSFT'
        
        # Test with dots
        assert _norm_symbol_us('BRK.A') == 'BRK.A'
        assert _norm_symbol_us('BRK.B') == 'BRK.B'
    
    def test_norm_symbol_hk(self):
        """Test HK symbol normalization."""
        # Test basic HK symbols
        assert _norm_symbol_hk('0700') == '0700.HK'
        assert _norm_symbol_hk('700') == '0700.HK'
        assert _norm_symbol_hk('7') == '0007.HK'
        
        # Test with .HK suffix
        assert _norm_symbol_hk('0700.HK') == '0700.HK'
        assert _norm_symbol_hk('700.HK') == '0700.HK'
        
        # Test edge cases
        assert _norm_symbol_hk('1') == '0001.HK'
        assert _norm_symbol_hk('9999') == '9999.HK'


class TestYFinanceDataConversion:
    """Test data conversion functionality."""
    
    def test_to_records_daily(self):
        """Test daily data conversion."""
        # Create sample daily data
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Open': [100.0, 101.0, 102.0],
            'High': [105.0, 106.0, 107.0],
            'Low': [95.0, 96.0, 97.0],
            'Close': [103.0, 104.0, 105.0],
            'Volume': [1000000, 1100000, 1200000]
        })
        
        result = _to_records(df, 'Date')
        
        assert isinstance(result, pd.DataFrame)
        assert 'date' in result.columns
        assert 'open' in result.columns
        assert 'high' in result.columns
        assert 'low' in result.columns
        assert 'close' in result.columns
        assert 'volume' in result.columns
        assert len(result) == 3
    
    def test_to_records_minute(self):
        """Test minute data conversion."""
        # Create sample minute data
        df = pd.DataFrame({
            'Datetime': pd.date_range('2024-01-01 09:30:00', periods=3, freq='5T'),
            'Open': [100.0, 101.0, 102.0],
            'High': [105.0, 106.0, 107.0],
            'Low': [95.0, 96.0, 97.0],
            'Close': [103.0, 104.0, 105.0],
            'Volume': [100000, 110000, 120000]
        })
        
        result = _to_records(df, 'Datetime')
        
        assert isinstance(result, pd.DataFrame)
        assert 'datetime' in result.columns
        assert 'open' in result.columns
        assert 'high' in result.columns
        assert 'low' in result.columns
        assert 'close' in result.columns
        assert 'volume' in result.columns
        assert len(result) == 3


class TestYFinanceUSMarket:
    """Test US market functionality."""
    
    @patch('ak_unified.adapters.yfinance_adapter._import_yf')
    def test_call_yfinance_us_daily(self, mock_import_yf):
        """Test US daily data fetching."""
        mock_yf = MagicMock()
        mock_import_yf.return_value = mock_yf
        
        # Mock download response
        mock_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=2),
            'Open': [100.0, 101.0],
            'High': [105.0, 106.0],
            'Low': [95.0, 96.0],
            'Close': [103.0, 104.0],
            'Volume': [1000000, 1100000]
        })
        mock_yf.download.return_value = mock_df
        
        result = call_yfinance(
            'securities.equity.us.ohlcv_daily.yf',
            {'symbol': 'AAPL', 'start': '2024-01-01', 'end': '2024-01-02'}
        )
        
        assert result[0] == 'yfinance.download_1d'
        assert isinstance(result[1], pd.DataFrame)
        assert 'symbol' in result[1].columns
        assert result[1]['symbol'].iloc[0] == 'AAPL'
        
        # Verify yfinance.download was called correctly
        mock_yf.download.assert_called_once_with(
            'AAPL', start='2024-01-01', end='2024-01-02',
            interval='1d', auto_adjust=False, progress=False
        )
    
    @patch('ak_unified.adapters.yfinance_adapter._import_yf')
    def test_call_yfinance_us_daily_error(self, mock_import_yf):
        """Test US daily data fetching error handling."""
        mock_yf = MagicMock()
        mock_import_yf.return_value = mock_yf
        
        mock_yf.download.side_effect = Exception("Network error")
        
        with pytest.raises(YFAdapterError) as exc_info:
            call_yfinance(
                'securities.equity.us.ohlcv_daily.yf',
                {'symbol': 'AAPL'}
            )
        
        assert "Network error" in str(exc_info.value)


class TestYFinanceHKMarket:
    """Test HK market functionality."""
    
    @patch('ak_unified.adapters.yfinance_adapter._import_yf')
    def test_call_yfinance_hk_daily(self, mock_import_yf):
        """Test HK daily data fetching."""
        mock_yf = MagicMock()
        mock_import_yf.return_value = mock_yf
        
        # Mock download response
        mock_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=2),
            'Open': [100.0, 101.0],
            'High': [105.0, 106.0],
            'Low': [95.0, 96.0],
            'Close': [103.0, 104.0],
            'Volume': [1000000, 1100000]
        })
        mock_yf.download.return_value = mock_df
        
        result = call_yfinance(
            'securities.equity.hk.ohlcv_daily.yf',
            {'symbol': '0700', 'start': '2024-01-01', 'end': '2024-01-02'}
        )
        
        assert result[0] == 'yfinance.download_1d'
        assert isinstance(result[1], pd.DataFrame)
        assert 'symbol' in result[1].columns
        assert result[1]['symbol'].iloc[0] == '0700.HK'
        
        # Verify yfinance.download was called correctly
        mock_yf.download.assert_called_once_with(
            '0700.HK', start='2024-01-01', end='2024-01-02',
            interval='1d', auto_adjust=False, progress=False
        )


class TestYFinanceMinuteData:
    """Test minute data functionality."""
    
    @patch('ak_unified.adapters.yfinance_adapter._import_yf')
    def test_call_yfinance_us_minute(self, mock_import_yf):
        """Test US minute data fetching."""
        mock_yf = MagicMock()
        mock_import_yf.return_value = mock_yf
        
        # Mock download response
        mock_df = pd.DataFrame({
            'Datetime': pd.date_range('2024-01-01 09:30:00', periods=2, freq='5T'),
            'Open': [100.0, 101.0],
            'High': [105.0, 106.0],
            'Low': [95.0, 96.0],
            'Close': [103.0, 104.0],
            'Volume': [100000, 110000]
        })
        mock_yf.download.return_value = mock_df
        
        result = call_yfinance(
            'securities.equity.us.ohlcv_min.yf',
            {'symbol': 'AAPL', 'freq': 'min5'}
        )
        
        assert result[0] == 'yfinance.download_5m'
        assert isinstance(result[1], pd.DataFrame)
        assert 'symbol' in result[1].columns
        assert result[1]['symbol'].iloc[0] == 'AAPL'
        
        # Verify yfinance.download was called correctly
        mock_yf.download.assert_called_once_with(
            'AAPL', period='60d', interval='5m',
            auto_adjust=False, progress=False
        )
    
    @patch('ak_unified.adapters.yfinance_adapter._import_yf')
    def test_call_yfinance_hk_minute(self, mock_import_yf):
        """Test HK minute data fetching."""
        mock_yf = MagicMock()
        mock_import_yf.return_value = mock_yf
        
        # Mock download response
        mock_df = pd.DataFrame({
            'Datetime': pd.date_range('2024-01-01 09:30:00', periods=2, freq='5T'),
            'Open': [100.0, 101.0],
            'High': [105.0, 106.0],
            'Low': [95.0, 96.0],
            'Close': [103.0, 104.0],
            'Volume': [100000, 110000]
        })
        mock_yf.download.return_value = mock_df
        
        result = call_yfinance(
            'securities.equity.hk.ohlcv_min.yf',
            {'symbol': '0700', 'freq': 'min15'}
        )
        
        assert result[0] == 'yfinance.download_15m'
        assert isinstance(result[1], pd.DataFrame)
        assert 'symbol' in result[1].columns
        assert result[1]['symbol'].iloc[0] == '0700.HK'
        
        # Verify yfinance.download was called correctly
        mock_yf.download.assert_called_once_with(
            '0700.HK', period='60d', interval='15m',
            auto_adjust=False, progress=False
        )
    
    def test_frequency_mapping(self):
        """Test frequency to interval mapping."""
        # Test various frequency mappings
        test_cases = [
            ('min1', '1m'), ('1', '1m'),
            ('min5', '5m'), ('5', '5m'),
            ('min15', '15m'), ('15', '15m'),
            ('min30', '30m'), ('30', '30m'),
            ('min60', '60m'), ('60', '60m'),
            ('unknown', '5m')  # default
        ]
        
        for freq, expected_interval in test_cases:
            with patch('ak_unified.adapters.yfinance_adapter._import_yf') as mock_import_yf:
                mock_yf = MagicMock()
                mock_import_yf.return_value = mock_yf
                
                mock_df = pd.DataFrame({
                    'Datetime': pd.date_range('2024-01-01 09:30:00', periods=1, freq='5T'),
                    'Open': [100.0], 'High': [105.0], 'Low': [95.0],
                    'Close': [103.0], 'Volume': [100000]
                })
                mock_yf.download.return_value = mock_df
                
                result = call_yfinance(
                    'securities.equity.us.ohlcv_min.yf',
                    {'symbol': 'AAPL', 'freq': freq}
                )
                
                # Extract interval from result function name
                actual_interval = result[0].split('_')[-1]
                assert actual_interval == expected_interval


class TestYFinanceQuotes:
    """Test quote functionality."""
    
    @patch('ak_unified.adapters.yfinance_adapter._import_yf')
    def test_call_yfinance_single_quote(self, mock_import_yf):
        """Test single symbol quote fetching."""
        mock_yf = MagicMock()
        mock_import_yf.return_value = mock_yf
        
        # Mock download response for single symbol
        mock_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=1),
            'Open': [100.0], 'High': [105.0], 'Low': [95.0],
            'Close': [103.0], 'Volume': [1000000]
        })
        mock_yf.download.return_value = mock_df
        
        result = call_yfinance(
            'securities.equity.us.quote.yf',
            {'symbol': 'AAPL'}
        )
        
        assert result[0] == 'yfinance.download_quote_multi'
        assert isinstance(result[1], pd.DataFrame)
        assert 'symbol' in result[1].columns
        assert result[1]['symbol'].iloc[0] == 'AAPL'
    
    @patch('ak_unified.adapters.yfinance_adapter._import_yf')
    def test_call_yfinance_multi_quote(self, mock_import_yf):
        """Test multiple symbols quote fetching."""
        mock_yf = MagicMock()
        mock_import_yf.return_value = mock_yf
        
        # Mock download response for multiple symbols
        mock_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=1),
            'Open': [100.0], 'High': [105.0], 'Low': [95.0],
            'Close': [103.0], 'Volume': [1000000]
        })
        
        # Create multi-level DataFrame for multiple symbols
        multi_df = pd.concat([mock_df, mock_df], axis=1, keys=['AAPL', 'MSFT'])
        mock_yf.download.return_value = multi_df
        
        result = call_yfinance(
            'securities.equity.us.quote.yf',
            {'symbols': ['AAPL', 'MSFT']}
        )
        
        assert result[0] == 'yfinance.download_quote_multi'
        assert isinstance(result[1], pd.DataFrame)
        assert 'symbol' in result[1].columns
        assert len(result[1]) == 2
        assert 'AAPL' in result[1]['symbol'].values
        assert 'MSFT' in result[1]['symbol'].values


class TestYFinanceErrorHandling:
    """Test error handling scenarios."""
    
    def test_yf_adapter_error_inheritance(self):
        """Test YF adapter error inheritance."""
        error = YFAdapterError("Test error")
        assert isinstance(error, RuntimeError)
        assert str(error) == "Test error"
    
    @patch('ak_unified.adapters.yfinance_adapter._import_yf')
    def test_empty_dataframe_handling(self, mock_import_yf):
        """Test handling of empty DataFrame returns."""
        mock_yf = MagicMock()
        mock_import_yf.return_value = mock_yf
        
        # Mock empty DataFrame return
        mock_yf.download.return_value = pd.DataFrame()
        
        result = call_yfinance(
            'securities.equity.us.ohlcv_daily.yf',
            {'symbol': 'INVALID'}
        )
        
        assert result[0] == 'yfinance.download_1d'
        assert isinstance(result[1], pd.DataFrame)
        assert result[1].empty
    
    @patch('ak_unified.adapters.yfinance_adapter._import_yf')
    def test_non_dataframe_return_handling(self, mock_import_yf):
        """Test handling of non-DataFrame returns."""
        mock_yf = MagicMock()
        mock_import_yf.return_value = mock_yf
        
        # Mock non-DataFrame return
        mock_yf.download.return_value = "invalid_data"
        
        result = call_yfinance(
            'securities.equity.us.ohlcv_daily.yf',
            {'symbol': 'AAPL'}
        )
        
        assert result[0] == 'yfinance.download_1d'
        assert isinstance(result[1], pd.DataFrame)
        assert result[1].empty


if __name__ == "__main__":
    pytest.main([__file__, "-v"])