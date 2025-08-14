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
- Complementary adapters: baostock, mootdx (Windows偏好), qmt (Windows-only), efinance, qstock, adata, yfinance, Alpha Vantage

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

## US/HK data sources
- yfinance（可选安装 `uv add yfinance` 或 `uv sync --extra yfinance`）
  - US/HK: `securities.equity.{us|hk}.ohlcv_daily.yf` / `.ohlcv_min.yf` / `.quote.yf`
  - 无 amount 字段；分钟级受 60d/区间限制
- Alpha Vantage（无需额外包，需 API Key）
  - 设置环境变量：`AKU_ALPHAVANTAGE_API_KEY` 或 `ALPHAVANTAGE_API_KEY`
  - US/HK: `securities.equity.{us|hk}.ohlcv_daily.av` / `.ohlcv_min.av` / `.quote.av`
  - 速率限制较严格；Note/Error 情况将返回空结果

AkShare 对港股：已实现 `quote`、`ohlcv_daily`、财报/指标等；分钟级 `ohlcv_min` 由 yfinance/Alpha Vantage 互补。

## Normalization
- 系统内置按数据集前缀的标准化规则：时间字段格式化、symbol 大写、常用数值字段转 float 等；并在响应和存储前统一应用
- 可通过 `AKU_NORMALIZATION_RULES`