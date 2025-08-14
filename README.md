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
- Board aggregation snapshot: `/rpc/agg/board_snapshot?board_kind=industry&boards=半导体&topn=5&weight_by=amount` (weight_by: none|amount|weight)
- Index aggregation snapshot: `/rpc/agg/index_snapshot?index_codes=000300.SH&topn=5&weight_by=weight` (当成分含权重列时可用)
- Aggregation playback: `/rpc/agg/playback?entity_type=board&ids=半导体&freq=min5&window_n=10`

SSE topics:
- Generic stream: `/topic/stream?dataset_id=securities.equity.cn.quote&interval=2.0`
- QMT board aggregation: `/topic/qmt/board?board_kind=industry&interval=2&window_n=10&bucket_sec=60&history_buckets=30&adapter_priority=qmt&adapter_priority=akshare&adapter_priority=qstock`
- QMT index aggregation: `/topic/qmt/index?index_codes=000300.SH&adapter_priority=qmt&adapter_priority=akshare`
- Board aggregation (polling): `/topic/board?board_kind=industry&boards=半导体&interval=2&window_n=10&topn=5&bucket_sec=60&history_buckets=30`
- Index aggregation (polling): `/topic/index?index_codes=000300.SH&interval=2&window_n=10&topn=5&bucket_sec=60&history_buckets=30`

## Normalization
- 系统内置按数据集前缀的标准化规则：时间字段格式化、symbol 大写、常用数值字段转 float 等；并在响应和存储前统一应用
- 可通过 `AKU_NORMALIZATION_RULES`（JSON 数组）覆盖/扩展前缀规则，例如：
```json
[
  {"prefix":"securities.equity.cn.ohlcva_daily","keep_fields":["symbol","date","open","high","low","close","volume","amount"]},
  {"prefix":"market.index","drop_fields":["turnover_rate"],"rename_map":{"收盘":"close"}}
]
```

## Postgres caching (asyncpg)
- 设置 `AKU_DB_DSN` 启用；可选 TTL：`AKU_CACHE_TTL_SECONDS` 或 `AKU_CACHE_TTL_PER_DATASET`
- 查询流程：先查库；若部分或全部缺失，将从上游获取数据并合并回写（SSE 实时来源暂不缓存）

Export/Import cache:
```bash
uv run python -m ak_unified.tools.cache_tools export -o cache.ndjson --dataset-prefix market.index --time-field date --start 2024-01-01 --end 2024-06-30 \
  --rename-map-json '{"收盘":"close"}' --keep-fields symbol,date,open,high,low,close,volume,amount
uv run python -m ak_unified.tools.cache_tools import -i cache.ndjson --drop-fields pct_change,turnover_rate
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