"""
Comprehensive tests for AkShare functions based on official documentation examples.
These tests validate the core functionality of AkShare data fetching through ak-unified.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from ak_unified.adapters.akshare_adapter import call_akshare, ak_function_vendor


class TestAkShareBasicFunctions:
    """Test basic AkShare functions that are commonly used."""
    
    @pytest.mark.asyncio
    async def test_stock_zh_a_hist(self):
        """Test stock_zh_a_hist function - A股历史行情数据"""
        # Mock AkShare module
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_a_hist.return_value = pd.DataFrame({
                '日期': ['2024-01-01', '2024-01-02'],
                '开盘': [10.0, 10.1],
                '收盘': [10.1, 10.2],
                '最高': [10.2, 10.3],
                '最低': [9.9, 10.0],
                '成交量': [1000000, 1100000],
                '成交额': [10000000, 11000000],
                '振幅': [2.0, 2.1],
                '涨跌幅': [1.0, 1.0],
                '涨跌额': [0.1, 0.1],
                '换手率': [0.5, 0.55]
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['stock_zh_a_hist'],
                {'symbol': '000001', 'start_date': '20240101', 'end_date': '20240102'},
                function_name='stock_zh_a_hist'
            )
            
            assert result[0] == 'stock_zh_a_hist'
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2
            assert 'date' in result[1].columns or '日期' in result[1].columns
    
    @pytest.mark.asyncio
    async def test_stock_zh_a_hist_min_em(self):
        """Test stock_zh_a_hist_min_em function - A股分钟级历史行情数据"""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_a_hist_min_em.return_value = pd.DataFrame({
                '时间': ['09:30:00', '09:31:00'],
                '开盘': [10.0, 10.1],
                '收盘': [10.1, 10.2],
                '最高': [10.2, 10.3],
                '最低': [9.9, 10.0],
                '成交量': [100000, 110000],
                '成交额': [1000000, 1100000]
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['stock_zh_a_hist_min_em'],
                {'symbol': '000001', 'period': '1', 'start_date': '20240101', 'end_date': '20240101'},
                function_name='stock_zh_a_hist_min_em'
            )
            
            assert result[0] == 'stock_zh_a_hist_min_em'
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2
    
    @pytest.mark.asyncio
    async def test_stock_zh_a_spot_em(self):
        """Test stock_zh_a_spot_em function - A股实时行情数据"""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame({
                '代码': ['000001', '000002'],
                '名称': ['平安银行', '万科A'],
                '最新价': [10.1, 20.2],
                '涨跌幅': [1.0, -0.5],
                '涨跌额': [0.1, -0.1],
                '成交量': [1000000, 2000000],
                '成交额': [10000000, 40000000],
                '振幅': [2.0, 1.5],
                '最高': [10.2, 20.3],
                '最低': [10.0, 20.1],
                '今开': [10.0, 20.2],
                '昨收': [10.0, 20.3],
                '换手率': [0.5, 0.8],
                '市盈率-动态': [15.2, 12.5],
                '市净率': [1.2, 1.8]
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['stock_zh_a_spot_em'],
                {},
                function_name='stock_zh_a_spot_em'
            )
            
            assert result[0] == 'stock_zh_a_spot_em'
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2
            assert '代码' in result[1].columns or 'code' in result[1].columns


class TestAkShareIndexFunctions:
    """Test AkShare index-related functions."""
    
    @pytest.mark.asyncio
    async def test_stock_zh_index_spot(self):
        """Test stock_zh_index_spot function - 股票指数实时行情"""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_index_spot.return_value = pd.DataFrame({
                '指数代码': ['000001', '000300'],
                '指数名称': ['上证指数', '沪深300'],
                '最新价': [3000.0, 4000.0],
                '涨跌幅': [0.5, 0.3],
                '涨跌额': [15.0, 12.0],
                '成交量': [1000000000, 800000000],
                '成交额': [100000000000, 80000000000]
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['stock_zh_index_spot'],
                {},
                function_name='stock_zh_index_spot'
            )
            
            assert result[0] == 'stock_zh_index_spot'
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2
    
    @pytest.mark.asyncio
    async def test_stock_zh_index_daily(self):
        """Test stock_zh_index_daily function - 股票指数历史行情"""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_index_daily.return_value = pd.DataFrame({
                '日期': ['2024-01-01', '2024-01-02'],
                '开盘': [3000.0, 3001.0],
                '收盘': [3001.0, 3002.0],
                '最高': [3002.0, 3003.0],
                '最低': [2999.0, 3000.0],
                '成交量': [1000000000, 1100000000]
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['stock_zh_index_daily'],
                {'symbol': '000001'},
                function_name='stock_zh_index_daily'
            )
            
            assert result[0] == 'stock_zh_index_daily'
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2


class TestAkShareFundFunctions:
    """Test AkShare fund-related functions."""
    
    @pytest.mark.asyncio
    async def test_fund_etf_hist_em(self):
        """Test fund_etf_hist_em function - ETF基金历史行情"""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.fund_etf_hist_em.return_value = pd.DataFrame({
                '日期': ['2024-01-01', '2024-01-02'],
                '开盘': [1.0, 1.01],
                '收盘': [1.01, 1.02],
                '最高': [1.02, 1.03],
                '最低': [0.99, 1.0],
                '成交量': [1000000, 1100000],
                '成交额': [1000000, 1100000],
                '振幅': [3.0, 3.1],
                '涨跌幅': [1.0, 1.0],
                '涨跌额': [0.01, 0.01]
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['fund_etf_hist_em'],
                {'symbol': '510300', 'start_date': '20240101', 'end_date': '20240102'},
                function_name='fund_etf_hist_em'
            )
            
            assert result[0] == 'fund_etf_hist_em'
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2


class TestAkShareBondFunctions:
    """Test AkShare bond-related functions."""
    
    @pytest.mark.asyncio
    async def test_bond_zh_hs_cov_spot(self):
        """Test bond_zh_hs_cov_spot function - 可转债实时行情"""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.bond_zh_hs_cov_spot.return_value = pd.DataFrame({
                '转债代码': ['110031', '110032'],
                '转债名称': ['航信转债', '海印转债'],
                '最新价': [120.5, 118.8],
                '涨跌幅': [0.5, -0.2],
                '涨跌额': [0.6, -0.2],
                '成交量': [100000, 150000],
                '成交额': [12000000, 17820000]
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['bond_zh_hs_cov_spot'],
                {},
                function_name='bond_zh_hs_cov_spot'
            )
            
            assert result[0] == 'bond_zh_hs_cov_spot'
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2


class TestAkShareMacroFunctions:
    """Test AkShare macro-economic functions."""
    
    @pytest.mark.asyncio
    async def test_macro_china_cpi_yearly(self):
        """Test macro_china_cpi_yearly function - 中国CPI年率"""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.macro_china_cpi_yearly.return_value = pd.DataFrame({
                '月份': ['2024-01', '2024-02'],
                'CPI年率': [2.1, 2.0],
                'CPI年率-预测值': [2.0, 2.1],
                'CPI年率-前值': [2.0, 2.1]
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['macro_china_cpi_yearly'],
                {},
                function_name='macro_china_cpi_yearly'
            )
            
            assert result[0] == 'macro_china_cpi_yearly'
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2
    
    @pytest.mark.asyncio
    async def test_macro_china_pmi(self):
        """Test macro_china_pmi function - 中国PMI"""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.macro_china_pmi.return_value = pd.DataFrame({
                '月份': ['2024-01', '2024-02'],
                '制造业PMI': [50.1, 50.2],
                '非制造业PMI': [51.0, 51.1],
                '综合PMI': [50.5, 50.6]
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['macro_china_pmi'],
                {},
                function_name='macro_china_pmi'
            )
            
            assert result[0] == 'macro_china_pmi'
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 2


class TestAkShareVendorDetection:
    """Test AkShare vendor detection functionality."""
    
    def test_ak_function_vendor_detection(self):
        """Test automatic vendor detection for AkShare functions."""
        # Test East Money functions
        assert ak_function_vendor('stock_zh_a_hist_em') == 'eastmoney'
        assert ak_function_vendor('stock_zh_a_spot_em') == 'eastmoney'
        assert ak_function_vendor('fund_etf_hist_em') == 'eastmoney'
        
        # Test Sina functions
        assert ak_function_vendor('stock_zh_a_hist_sina') == 'sina'
        assert ak_function_vendor('stock_zh_index_spot_sina') == 'sina'
        
        # Test Tencent functions
        assert ak_function_vendor('stock_zh_a_hist_tx') == 'tencent'
        assert ak_function_vendor('stock_zh_a_spot_tx') == 'tencent'
        
        # Test THS functions
        assert ak_function_vendor('stock_zh_a_hist_ths') == 'ths'
        assert ak_function_vendor('stock_zh_a_spot_ths') == 'ths'
        
        # Test TDX functions
        assert ak_function_vendor('stock_zh_a_hist_tdx') == 'tdx'
        assert ak_function_vendor('stock_zh_a_spot_tdx') == 'tdx'
        
        # Test Baidu functions
        assert ak_function_vendor('stock_zh_a_hist_baidu') == 'baidu'
        
        # Test NetEase functions
        assert ak_function_vendor('stock_zh_a_hist_netease') == 'netease'
        assert ak_function_vendor('stock_zh_a_hist_163') == 'netease'
        
        # Test Hexun functions
        assert ak_function_vendor('stock_zh_a_hist_hexun') == 'hexun'
        
        # Test CSIndex functions
        assert ak_function_vendor('stock_zh_a_hist_csindex') == 'csindex'
        
        # Test Jisilu functions
        assert ak_function_vendor('stock_zh_a_hist_jsl') == 'jisilu'
        assert ak_function_vendor('stock_zh_a_hist_jisilu') == 'jisilu'
        
        # Test unknown functions
        assert ak_function_vendor('unknown_function') == 'unknown'


class TestAkShareErrorHandling:
    """Test AkShare error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_function_not_found_error(self):
        """Test error handling when AkShare function doesn't exist."""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            # Function doesn't exist
            mock_ak.nonexistent_function = None
            mock_import.return_value = mock_ak
            
            with pytest.raises(Exception) as exc_info:
                await call_akshare(
                    ['nonexistent_function'],
                    {},
                    function_name='nonexistent_function'
                )
            
            assert 'AkShare function not found' in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_empty_dataframe_return(self):
        """Test handling of empty DataFrame returns."""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_a_hist.return_value = pd.DataFrame()
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['stock_zh_a_hist'],
                {'symbol': '000001'},
                function_name='stock_zh_a_hist'
            )
            
            assert result[0] == 'stock_zh_a_hist'
            assert isinstance(result[1], pd.DataFrame)
            assert result[1].empty
    
    @pytest.mark.asyncio
    async def test_fallback_functionality(self):
        """Test fallback functionality when multiple functions are available."""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            
            # First function returns empty DataFrame
            mock_ak.stock_zh_a_hist.return_value = pd.DataFrame()
            
            # Second function returns data
            mock_ak.stock_zh_a_hist_em.return_value = pd.DataFrame({
                '日期': ['2024-01-01'],
                '开盘': [10.0],
                '收盘': [10.1],
                '最高': [10.2],
                '最低': [9.9],
                '成交量': [1000000]
            })
            
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['stock_zh_a_hist', 'stock_zh_a_hist_em'],
                {'symbol': '000001'},
                allow_fallback=True
            )
            
            assert result[0] == 'stock_zh_a_hist_em'
            assert isinstance(result[1], pd.DataFrame)
            assert len(result[1]) == 1


class TestAkShareDataTransformation:
    """Test data transformation and normalization."""
    
    @pytest.mark.asyncio
    async def test_column_renaming(self):
        """Test field mapping functionality."""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_a_hist.return_value = pd.DataFrame({
                '日期': ['2024-01-01'],
                '开盘': [10.0],
                '收盘': [10.1],
                '最高': [10.2],
                '最低': [9.9],
                '成交量': [1000000]
            })
            mock_import.return_value = mock_ak
            
            field_mapping = {
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume'
            }
            
            result = await call_akshare(
                ['stock_zh_a_hist'],
                {'symbol': '000001'},
                field_mapping=field_mapping,
                function_name='stock_zh_a_hist'
            )
            
            assert result[0] == 'stock_zh_a_hist'
            df = result[1]
            assert 'date' in df.columns
            assert 'open' in df.columns
            assert 'close' in df.columns
            assert 'high' in df.columns
            assert 'low' in df.columns
            assert 'volume' in df.columns
    
    @pytest.mark.asyncio
    async def test_symbol_column_addition(self):
        """Test automatic symbol column addition."""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_a_hist.return_value = pd.DataFrame({
                '日期': ['2024-01-01'],
                '开盘': [10.0],
                '收盘': [10.1]
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['stock_zh_a_hist'],
                {'symbol': '000001'},
                function_name='stock_zh_a_hist'
            )
            
            assert result[0] == 'stock_zh_a_hist'
            df = result[1]
            assert 'symbol' in df.columns
            assert df['symbol'].iloc[0] == '000001'
    
    @pytest.mark.asyncio
    async def test_data_type_normalization(self):
        """Test automatic data type normalization."""
        with patch('ak_unified.adapters.akshare_adapter._import_akshare') as mock_import:
            mock_ak = MagicMock()
            mock_ak.stock_zh_a_hist.return_value = pd.DataFrame({
                '日期': ['2024-01-01'],
                '开盘': ['10.0'],  # String instead of float
                '收盘': ['10.1'],  # String instead of float
                '成交量': ['1000000']  # String instead of int
            })
            mock_import.return_value = mock_ak
            
            result = await call_akshare(
                ['stock_zh_a_hist'],
                {'symbol': '000001'},
                function_name='stock_zh_a_hist'
            )
            
            assert result[0] == 'stock_zh_a_hist'
            df = result[1]
            
            # Check that numeric columns are converted to numeric types
            assert pd.api.types.is_numeric_dtype(df['开盘'])
            assert pd.api.types.is_numeric_dtype(df['收盘'])
            assert pd.api.types.is_numeric_dtype(df['成交量'])
            
            # Check that date column remains as string
            assert pd.api.types.is_string_dtype(df['日期'])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])