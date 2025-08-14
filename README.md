# ak-unified

Unified interface and schemas for AkShare across macro, market, and securities categories. Managed by `uv`.

## Setup
```bash
uv venv
uv sync
uv run python -c "import ak_unified as aku; print(aku.__version__)"
```

## Structure
- `src/ak_unified/schemas`: Pydantic models for envelopes and domain schemas
- `src/ak_unified/registry.py`: Dataset registry and computed datasets
- `src/ak_unified/adapters/*`: Adapters for akshare/baostock/mootdx/qmt/efinance/qstock/adata
- `src/ak_unified/dispatcher.py`: Unified entrypoints like `fetch_data`, `get_ohlcv`, `get_ohlcva`
- `src/ak_unified/api.py`: FastAPI app exposing RPC and SSE topics

## Key features
- Unified envelope schema with metadata (`ak_function`, `data_source`)
- Explicit data-source selection via `adapter` and AkShare `ak_function`; optional fallback
- OHLCVA datasets including amount (成交额):
  - `securities.equity.cn.ohlcva_daily`, `securities.equity.cn.ohlcva_min`
  - `market.index.ohlcva`
  - `securities.board.cn.{industry,concept}.ohlcva_daily` and `.ohlcva_min`
- Computed datasets:
  - `market.cn.valuation_momentum.snapshot` (估值分位与动量，支持 index/board)
  - `market.cn.aggregation.playback`（指数/板块时序回放）
  - `market.cn.industry_weight_distribution`（指数行业权重分布，自动近似权重）
  - `market.cn.volume_percentile`（量能分位）
- Complementary adapters: baostock, mootdx (Windows偏好), qmt (Windows-only), efinance, qstock, adata

## FastAPI
Run:
```bash
uv run uvicorn ak_unified.api:app --reload
```
RPC examples:
- Fetch OHLCVA: `/rpc/ohlcva?symbol=600000.SH&start=2024-01-01&end=2024-01-31&adjust=none`
- Valuation & momentum: `/rpc/fetch?dataset_id=market.cn.valuation_momentum.snapshot&entity_type=index&ids=沪深300&window=60`
- Playback (board): `/rpc/fetch?dataset_id=market.cn.aggregation.playback&entity_type=board&ids=半导体&freq=min5&window_n=10`
- Industry weights: `/rpc/fetch?dataset_id=market.cn.industry_weight_distribution&index_code=000300.SH`
- Volume percentile: `/rpc/fetch?dataset_id=market.cn.volume_percentile&entity_type=index&ids=沪深300&lookback=120`

SSE topics:
- Generic stream: `/topic/stream?dataset_id=securities.equity.cn.quote&interval=2.0`
- QMT board aggregation: `/topic/qmt/board?board_kind=industry&interval=2&window_n=10&bucket_sec=60&history_buckets=30&adapter_priority=qmt&adapter_priority=akshare&adapter_priority=qstock`
- QMT index aggregation: `/topic/qmt/index?index_codes=000300.SH&adapter_priority=qmt&adapter_priority=akshare`

## Testing
Run tests:
```bash
uv run pytest -q
```

## Notes
- Field names are normalized to snake_case and English.
- Timezone defaults to Asia/Shanghai unless otherwise specified.
- Some upstream endpoints may change; switch adapters or specify `ak_function` as needed.