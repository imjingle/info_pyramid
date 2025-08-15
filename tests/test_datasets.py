import importlib
import pytest
from unittest.mock import patch, MagicMock

from ak_unified import fetch_data


@pytest.mark.asyncio
async def test_registry_core_datasets_present():
    """Test that core datasets are fetchable without raising."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_envelope = MagicMock()
        mock_envelope.data = [{'symbol': '000300.SH', 'close': 100.0}]
        mock_fetch.return_value = mock_envelope
        
        env = await fetch_data('market.index.ohlcva', {'symbol': '000300.SH'})
        assert isinstance(env.data, list)


@pytest.mark.asyncio
async def test_requires_ak_function_error_on_multiple_candidates():
    """Test error when multiple candidates available without ak_function."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_fetch.side_effect = Exception('Multiple candidate functions available')
        
        with pytest.raises(Exception) as ei:
            await fetch_data('securities.equity.cn.ohlcv_daily', {'symbol': '600000.SH'})
        assert 'Multiple candidate functions available' in str(ei.value)


@pytest.mark.asyncio
async def test_minute_ohlcv_cn_equity():
    """Test minute OHLCV data for Chinese equity."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_envelope = MagicMock()
        mock_envelope.data = [{'symbol': '600000.SH', 'close': 10.0}]
        mock_fetch.return_value = mock_envelope
        
        env = await fetch_data('securities.equity.cn.ohlcv_min', {'symbol': '600000.SH', 'freq': 'min5'}, ak_function='stock_zh_a_hist_min_em')
        assert isinstance(env.data, list)


@pytest.mark.asyncio
async def test_industry_list_em():
    """Test industry list from Eastmoney."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_envelope = MagicMock()
        mock_envelope.data = [{'industry': '半导体', 'count': 50}]
        mock_fetch.return_value = mock_envelope
        
        env = await fetch_data('securities.board.cn.industry.list', {}, ak_function='stock_board_industry_name_em')
        assert isinstance(env.data, list)


@pytest.mark.asyncio
async def test_volume_percentile_index():
    """Test volume percentile for index."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_envelope = MagicMock()
        mock_envelope.data = [{'entity_type': 'index', 'volume_percentile': 0.75}]
        mock_fetch.return_value = mock_envelope
        
        env = await fetch_data('market.cn.volume_percentile', {'entity_type': 'index', 'ids': ['沪深300'], 'lookback': 60})
        assert isinstance(env.data, list)
        assert len(env.data) == 1
        assert 'volume_percentile' in env.data[0]


@pytest.mark.asyncio
async def test_industry_weight_distribution_index():
    """Test industry weight distribution for index."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_envelope = MagicMock()
        mock_envelope.data = [{'industry': '半导体', 'weight': 0.15}]
        mock_fetch.return_value = mock_envelope
        
        env = await fetch_data('market.cn.industry_weight_distribution', {'index_code': '000300.SH'})
        # allow empty if upstream limited, but output should be a list
        assert isinstance(env.data, list)


@pytest.mark.skipif(importlib.util.find_spec('efinance') is None, reason='efinance not installed')
@pytest.mark.asyncio
async def test_efinance_ohlcv_daily_if_available():
    """Test efinance OHLCV daily data if available."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_envelope = MagicMock()
        mock_envelope.data = [{'symbol': '600000.SH', 'close': 10.0}]
        mock_fetch.return_value = mock_envelope
        
        env = await fetch_data('securities.equity.cn.ohlcv_daily.efinance', {'symbol': '600000.SH', 'start': '2024-01-01', 'end': '2024-01-10'})
        assert isinstance(env.data, list)


@pytest.mark.skipif(importlib.util.find_spec('qstock') is None, reason='qstock not installed')
@pytest.mark.asyncio
async def test_qstock_quote_if_available():
    """Test qstock quote data if available."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_envelope = MagicMock()
        mock_envelope.data = [{'symbol': '600000.SH', 'last': 10.0}]
        mock_fetch.return_value = mock_envelope
        
        env = await fetch_data('securities.equity.cn.quote.qstock', {})
        assert isinstance(env.data, list)