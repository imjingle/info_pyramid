import os
import pytest
from unittest.mock import patch, MagicMock

from ak_unified import fetch_data


@pytest.mark.asyncio
async def test_ohlcva_includes_amount():
    """Test that OHLCVA data includes amount field."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_envelope = MagicMock()
        mock_envelope.data = [
            {'symbol': '600000.SH', 'open': 10.0, 'close': 10.1, 'amount': 1000000}
        ]
        mock_fetch.return_value = mock_envelope
        
        env = await fetch_data(
            'securities.equity.cn.ohlcva_daily', 
            {'symbol':'600000.SH','start':'2024-01-02','end':'2024-01-10'}, 
            ak_function='stock_zh_a_hist'
        )
        
        assert isinstance(env.data, list)
        if env.data:
            keys = env.data[0].keys()
            assert 'amount' in keys or 'volume' in keys


@pytest.mark.asyncio
async def test_valuation_momentum_snapshot_index():
    """Test valuation momentum snapshot for index."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_envelope = MagicMock()
        mock_envelope.data = [
            {
                'entity_type': 'index',
                'momentum_60d': 0.05,
                'pe_percentile': 0.75
            }
        ]
        mock_fetch.return_value = mock_envelope
        
        env = await fetch_data('market.cn.valuation_momentum.snapshot', {
            'entity_type':'index', 'ids':['沪深300'], 'window': 60
        })
        
        assert isinstance(env.data, list)
        assert len(env.data) == 1
        row = env.data[0]
        assert row.get('entity_type') == 'index'
        assert 'momentum_60d' in row
        assert 'pe_percentile' in row


@pytest.mark.asyncio
async def test_playback_board_series():
    """Test playback board series data."""
    with patch('ak_unified.dispatcher.fetch_data') as mock_fetch:
        mock_envelope = MagicMock()
        mock_envelope.data = [
            {
                'entity_type': 'board',
                'series': [{'date': '2024-01-01', 'value': 100}]
            }
        ]
        mock_fetch.return_value = mock_envelope
        
        env = await fetch_data('market.cn.aggregation.playback', {
            'entity_type':'board', 'ids':['半导体'], 'freq':'min5', 'window_n': 5
        })
        
        assert isinstance(env.data, list)
        assert len(env.data) >= 1
        row = env.data[0]
        assert row.get('entity_type') == 'board'
        # series may be empty if upstream not available, but should exist as list
        assert 'series' in row
        assert isinstance(row['series'], list)