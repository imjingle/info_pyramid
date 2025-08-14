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
- Complementary adapters: baostock, mootdx (Windows偏好), qmt (Windows-only), efinance, qstock, adata, yfinance, Alpha Vantage, IBKR

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
- Fundamentals (CN):
  - Indicators: `/rpc/fetch?dataset_id=securities.equity.cn.fundamentals.indicators&symbol=600000.SH&ak_function=stock_financial_analysis_indicator_em`
  - Score: `/rpc/fetch?dataset_id=securities.equity.cn.fundamentals.score&symbol=600000.SH`
  - Snapshot: `/rpc/fetch?dataset_id=securities.equity.cn.fundamentals.snapshot&symbol=600000.SH`
- Fundamentals (HK):
  - Indicators: `/rpc/fetch?dataset_id=securities.equity.hk.fundamentals.indicators&symbol=00001.HK`
  - Snapshot: `/rpc/fetch?dataset_id=securities.equity.hk.fundamentals.snapshot&symbol=00001.HK`
- Fundamentals (US):
  - Indicators: `/rpc/fetch?dataset_id=securities.equity.us.fundamentals.indicators&symbol=AAPL`
  - IBKR Overview/Statements/Ratios: `/rpc/fetch?dataset_id=securities.equity.us.fundamentals.overview.ibkr&symbol=AAPL`
  - Alpha Vantage Fundamentals: `/rpc/fetch?dataset_id=securities.equity.us.fundamentals.income_statement.av&symbol=AAPL&period=annual`
  - Snapshot: `/rpc/fetch?dataset_id=securities.equity.us.fundamentals.snapshot&symbol=AAPL`
- Market lists and indices:
  - A-share list: `/rpc/fetch?dataset_id=securities.equity.cn.list`
  - Index list: `/rpc/fetch?dataset_id=market.index.cn.list`
  - Index spot (EM/Sina): `/rpc/fetch?dataset_id=market.index.cn.spot&symbol=上证系列指数`
  - Index spot (multi-source): `/rpc/fetch?dataset_id=market.index.cn.spot.multi&symbol=上证系列指数`
  - Index constituents (EM): `/rpc/fetch?dataset_id=market.index.constituents&index_code=000300`
  - Index constituents (CSIndex): `/rpc/fetch?dataset_id=market.index.constituents.csindex&index_code=000300`
  - Index weights (CSIndex): `/rpc/fetch?dataset_id=market.index.constituents_weight.csindex&index_code=000300`
  - Index detail (CNI): `/rpc/fetch?dataset_id=market.index.cni.detail&index_code=H11001`
  - Index constituents (multi): `/rpc/fetch?dataset_id=market.index.constituents.multi&index_code=000300`
- Boards and concepts:
  - Concept list (EM): `/rpc/fetch?dataset_id=securities.board.cn.concept.name_em`
  - Concept spot (EM): `/rpc/fetch?dataset_id=securities.board.cn.concept.spot_em`
  - Concept constituents (EM): `/rpc/fetch?dataset_id=securities.board.cn.concept.cons_em&symbol=半导体`
  - Concept history (EM): `/rpc/fetch?dataset_id=securities.board.cn.concept.hist_em&symbol=半导体&start=2024-01-01&end=2024-06-30`
  - Concept list (THS): `/rpc/fetch?dataset_id=securities.board.cn.concept.name_ths`
  - Concept index (THS): `/rpc/fetch?dataset_id=securities.board.cn.concept.index_ths&symbol=半导体`
  - Concept info (THS): `/rpc/fetch?dataset_id=securities.board.cn.concept.info_ths&symbol=半导体`
- Volatility (QVIX):
  - Daily: `/rpc/fetch?dataset_id=market.volatility.cn.qvix&ak_function=index_option_300etf_qvix`
  - Minute: `/rpc/fetch?dataset_id=market.volatility.cn.qvix_min&ak_function=index_option_300etf_min_qvix`
- Sentiment:
  - News sentiment scope: `/rpc/fetch?dataset_id=market.cn.news_sentiment.scope`
- Funds:
  - List: `/rpc/fetch?dataset_id=securities.fund.cn.list`
  - Basic info: `/rpc/fetch?dataset_id=securities.fund.cn.basic_info&fund_code=510300`
  - Spot ETF: `/rpc/fetch?dataset_id=securities.fund.cn.spot.etf`
  - ETF minute: `/rpc/fetch?dataset_id=securities.fund.cn.min.etf&fund_code=510300&start=09:30&end=15:00`
  - ETF history: `/rpc/fetch?dataset_id=securities.fund.cn.hist.etf&fund_code=510300`
  - Dividends: `/rpc/fetch?dataset_id=securities.fund.cn.dividend&fund_code=510300`
  - Fee: `/rpc/fetch?dataset_id=securities.fund.cn.fee&fund_code=510300`
  - Reports: `/rpc/fetch?dataset_id=securities.fund.cn.reports&fund_code=510300`
- Bonds:
  - List: `/rpc/fetch?dataset_id=securities.bond.cn.list`
  - Detail: `/rpc/fetch?dataset_id=securities.bond.cn.info&bond_code=110031`
  - Spot: `/rpc/fetch?dataset_id=securities.bond.cn.spot`
  - Hist: `/rpc/fetch?dataset_id=securities.bond.cn.hist&bond_code=110031&start=2024-01-01&end=2024-06-30`
  - Yield curves: `/rpc/fetch?dataset_id=securities.bond.cn.yield_curves`

SSE topics:
- Generic stream: `/topic/stream?dataset_id=securities.equity.cn.quote&interval=2.0`
- QMT board aggregation: `/topic/qmt/board?board_kind=industry&interval=2&window_n=10&bucket_sec=60&history_buckets=30&adapter_priority=qmt&adapter_priority=akshare&adapter_priority=qstock`
- QMT index aggregation: `/topic/qmt/index?index_codes=000300.SH&adapter_priority=qmt&adapter_priority=akshare`
- Board aggregation (polling): `/topic/board?board_kind=industry&boards=半导体&interval=2&window_n=10&topn=5&bucket_sec=60&history_buckets=30`
- Index aggregation (polling): `/topic/index?index_codes=000300.SH&interval=2&window_n=10&topn=5&bucket_sec=60&history_buckets=30`

## Markets coverage
- A 股（AkShare 为主，baostock/mootdx 等补充）：量价与多数基本面
- 港股：量价（AkShare 日线；分钟 yfinance/Alpha Vantage/IBKR）、基本面（AkShare/IBKR）
- 美股：量价（yfinance/Alpha Vantage/IBKR）、基本面（Alpha Vantage 概览/三表/盈利；IBKR 多报告 XML）

所有数据均通过统一的 `DataEnvelope` 返回，并在存储/返回前按数据集前缀进行 schema 归一（时间字段、标识字段、常用数值字段等）；US/HK `quote` 已统一基本字段：`symbol,last,prev_close,change,pct_change,bid,ask,volume`。

## US/HK data sources
- yfinance（可选安装 `uv add yfinance` 或 `uv sync --extra yfinance`）
  - US/HK: `securities.equity.{us|hk}.ohlcv_daily.yf` / `.ohlcv_min.yf` / `.quote.yf`
  - 无 amount 字段；分钟级受 60d/区间限制
- Alpha Vantage（无需额外包，需 API Key）
  - 设置环境变量：`AKU_ALPHAVANTAGE_API_KEY` 或 `ALPHAVANTAGE_API_KEY`
  - US/HK: `securities.equity.{us|hk}.ohlcv_daily.av` / `.ohlcv_min.av` / `.quote.av`
  - 速率限制较严格；Note/Error 情况将返回空结果
- IBKR（可选安装 `uv sync --extra ibkr`；需运行 TWS/IB Gateway 并允许 API）
  - 连接配置：`AKU_IB_HOST`（默认 127.0.0.1）、`AKU_IB_PORT`（默认 7497）、`AKU_IB_CLIENT_ID`（默认 1）
  - US/HK 行情：`securities.equity.{us|hk}.ohlcv_daily.ibkr` / `.ohlcv_min.ibkr` / `.quote.ibkr`
  - 基本面：`securities.equity.us.fundamentals.{overview|statements|ratios|snapshot}.ibkr`
  - 说明：基本面通过 `reqFundamentalData`（CompanyOverview/ReportsFinStatements/Ratios/ReportSnapshot）；行情历史通过 `reqHistoricalData`，分钟历史受限，按频率和 duration 动态选择；实时快照用 `reqMktData`

AkShare 对港股：已实现 `quote`、`ohlcv_daily`、财报/指标等；分钟级 `ohlcv_min` 由 yfinance/Alpha Vantage 互补。

## Normalization
- 系统内置按数据集前缀的标准化规则：时间字段格式化、symbol 大写、常用数值字段转 float 等；并在响应和存储前统一应用
- 可通过 `AKU_NORMALIZATION_RULES`