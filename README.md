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

## Postgres caching (asyncpg)
- 设置环境变量 `AKU_DB_DSN` 启用：例如 `export AKU_DB_DSN=postgres://user:pass@host:5432/dbname`
- 首次使用会自动初始化表 `aku_cache` 与必要索引
- 可选 TTL：全局 `AKU_CACHE_TTL_SECONDS`，或按数据集前缀 `AKU_CACHE_TTL_PER_DATASET`（JSON）
- 查询流程：先查库；若部分或全部缺失，将从上游获取数据并合并回写（SSE 实时来源暂不缓存）

Export/Import cache:
```bash
# export all datasets to ndjson
uv run python -m ak_unified.tools.cache_tools export -o cache.ndjson
# export specific prefix
uv run python -m ak_unified.tools.cache_tools export -o idx.ndjson --dataset-prefix market.index
# import from file
uv run python -m ak_unified.tools.cache_tools import -i cache.ndjson
# import with prefix filter
uv run python -m ak_unified.tools.cache_tools import -i idx.ndjson --dataset-prefix market.index
```

## Testing
Run tests:
```bash
uv run pytest -q
```

## Notes
- Field names are normalized to snake_case and English.
- Timezone defaults to Asia/Shanghai unless otherwise specified.
- Some upstream endpoints may change; switch adapters or specify `ak_function` as needed.