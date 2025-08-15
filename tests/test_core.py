import os
import pytest

from ak_unified import fetch_data

def test_ohlcva_includes_amount():
    env = fetch_data('securities.equity.cn.ohlcva_daily', {'symbol':'600000.SH','start':'2024-01-02','end':'2024-01-10'}, ak_function='stock_zh_a_hist')
    assert isinstance(env.data, list)
    if env.data:
        keys = env.data[0].keys()
        assert 'amount' in keys or 'volume' in keys


def test_valuation_momentum_snapshot_index():
    env = fetch_data('market.cn.valuation_momentum.snapshot', {
        'entity_type':'index', 'ids':['沪深300'], 'window': 60
    })
    assert isinstance(env.data, list)
    assert len(env.data) == 1
    row = env.data[0]
    assert row.get('entity_type') == 'index'
    assert 'momentum_60d' in row
    assert 'pe_percentile' in row


def test_playback_board_series():
    env = fetch_data('market.cn.aggregation.playback', {
        'entity_type':'board', 'ids':['半导体'], 'freq':'min5', 'window_n': 5
    })
    assert isinstance(env.data, list)
    assert len(env.data) >= 1
    row = env.data[0]
    assert row.get('entity_type') == 'board'
    # series may be empty if upstream not available, but should exist as list
    assert 'series' in row
    assert isinstance(row['series'], list)