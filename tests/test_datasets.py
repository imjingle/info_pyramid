import importlib
import pytest

from ak_unified import fetch_data


def test_registry_core_datasets_present():
    # core datasets should be fetchable without raising
    env = fetch_data('market.index.ohlcva', {'symbol': '000300.SH'})
    assert isinstance(env.data, list)


def test_requires_ak_function_error_on_multiple_candidates():
    # Without ak_function and no fallback, multi-candidate dataset should error
    with pytest.raises(Exception) as ei:
        fetch_data('securities.equity.cn.ohlcv_daily', {'symbol': '600000.SH'})
    assert 'Multiple candidate functions available' in str(ei.value)


def test_minute_ohlcv_cn_equity():
    env = fetch_data('securities.equity.cn.ohlcv_min', {'symbol': '600000.SH', 'freq': 'min5'}, ak_function='stock_zh_a_hist_min_em')
    assert isinstance(env.data, list)


def test_industry_list_em():
    env = fetch_data('securities.board.cn.industry.list', {}, ak_function='stock_board_industry_name_em')
    assert isinstance(env.data, list)


def test_volume_percentile_index():
    env = fetch_data('market.cn.volume_percentile', {'entity_type': 'index', 'ids': ['沪深300'], 'lookback': 60})
    assert isinstance(env.data, list)
    assert len(env.data) == 1
    assert 'volume_percentile' in env.data[0]


def test_industry_weight_distribution_index():
    env = fetch_data('market.cn.industry_weight_distribution', {'index_code': '000300.SH'})
    # allow empty if upstream limited, but output should be a list
    assert isinstance(env.data, list)


@pytest.mark.skipif(importlib.util.find_spec('efinance') is None, reason='efinance not installed')
def test_efinance_ohlcv_daily_if_available():
    env = fetch_data('securities.equity.cn.ohlcv_daily.efinance', {'symbol': '600000.SH', 'start': '2024-01-01', 'end': '2024-01-10'})
    assert isinstance(env.data, list)


@pytest.mark.skipif(importlib.util.find_spec('qstock') is None, reason='qstock not installed')
def test_qstock_quote_if_available():
    env = fetch_data('securities.equity.cn.quote.qstock', {})
    assert isinstance(env.data, list)