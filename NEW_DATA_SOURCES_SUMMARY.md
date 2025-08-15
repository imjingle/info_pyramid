# 新增数据源总结：Snowball 和 EasyTrader

本文档总结了新引入的两个数据源：**pysnowball**（雪球）和 **easytrader**（易交易）的完整实现情况。

## 🎯 **新增数据源概览**

### 1. **pysnowball - 雪球数据源**
- **GitHub**: https://github.com/uname-yang/pysnowball
- **功能**: 获取雪球网上的股票数据、财务数据、研报、用户讨论等
- **特点**: 数据全面，包含用户生成内容，社区氛围浓厚

### 2. **easytrader - 易交易数据源**
- **GitHub**: https://github.com/shidenggui/easytrader
- **功能**: 支持多个券商的交易接口，获取账户信息、持仓数据、交易记录等
- **特点**: 主要用于交易操作，但也包含市场数据

## 🏗️ **系统架构扩展**

### 1. **新增适配器**
```
src/ak_unified/adapters/
├── snowball_adapter.py      # 雪球数据适配器
└── easytrader_adapter.py    # 易交易数据适配器
```

### 2. **新增数据模型**
```
src/ak_unified/schemas/
├── snowball.py              # 雪球数据模型
└── easytrader.py            # 易交易数据模型
```

### 3. **新增 API 端点**
```
/rpc/snowball/*              # 雪球数据端点 (6个)
/rpc/easytrader/*            # 易交易数据端点 (7个)
```

## 🔧 **核心功能实现**

### 1. **Snowball 适配器 (SnowballAdapter)**

#### 主要功能
- **股票行情**: 获取实时股票报价和基本信息
- **财务数据**: 获取财务报表和关键指标
- **研报数据**: 获取机构研报和分析
- **情感分析**: 获取用户情感数据和讨论热度
- **社区讨论**: 获取用户讨论和观点
- **市场概览**: 获取市场整体情况

#### 核心方法
```python
async def get_stock_quote(self, symbol: str, market: str = 'cn') -> pd.DataFrame
async def get_stock_financial_data(self, symbol: str, market: str = 'cn', period: str = 'annual') -> pd.DataFrame
async def get_stock_research_reports(self, symbol: str, market: str = 'cn', limit: int = 20) -> pd.DataFrame
async def get_stock_sentiment(self, symbol: str, market: str = 'cn', days: int = 7) -> pd.DataFrame
async def get_stock_discussions(self, symbol: str, market: str = 'cn', limit: int = 50) -> pd.DataFrame
async def get_market_overview(self, market: str = 'cn') -> pd.DataFrame
```

#### 支持市场
- **中国 A 股** (`cn`)
- **港股** (`hk`)
- **美股** (`us`)

### 2. **EasyTrader 适配器 (EasyTraderAdapter)**

#### 主要功能
- **账户管理**: 登录、账户信息、资金状况
- **持仓管理**: 获取持仓明细、成本分析
- **交易记录**: 获取交易历史和成交明细
- **市场数据**: 获取实时行情和报价
- **风险指标**: 获取风险度量和分析

#### 支持券商
- **通用接口** (`universal`)
- **华泰证券** (`ht`)
- **银河证券** (`yh`)
- **广发证券** (`gf`)
- **雪球证券** (`xq`)
- **同花顺** (`ths`)

#### 核心方法
```python
async def login(self, username: str, password: str, exe_path: Optional[str] = None, comm_password: Optional[str] = None) -> bool
async def get_account_info(self) -> pd.DataFrame
async def get_portfolio(self) -> pd.DataFrame
async def get_trading_history(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame
async def get_market_data(self, symbols: List[str]) -> pd.DataFrame
async def get_fund_info(self) -> pd.DataFrame
async def get_risk_metrics(self) -> pd.DataFrame
```

## 📊 **数据模型设计**

### 1. **Snowball 数据模型**

#### SnowballQuote - 股票行情
```python
class SnowballQuote(BaseModel):
    symbol: str                           # 股票代码
    market: str                           # 市场代码
    name: str                             # 股票名称
    current: float                        # 当前价格
    change: float                         # 价格变动
    change_percent: float                 # 涨跌幅
    open: float                           # 开盘价
    high: float                           # 最高价
    low: float                            # 最低价
    volume: int                           # 成交量
    market_cap: float                     # 市值
    pe_ratio: float                       # 市盈率
    pb_ratio: float                       # 市净率
    dividend_yield: float                 # 股息率
```

#### SnowballResearchReport - 研报数据
```python
class SnowballResearchReport(BaseModel):
    symbol: str                           # 股票代码
    title: str                            # 研报标题
    author: str                           # 作者
    institution: str                      # 机构
    publish_date: str                     # 发布日期
    rating: str                           # 评级
    target_price: float                   # 目标价
    summary: str                          # 摘要
    url: str                              # 链接
```

#### SnowballSentiment - 情感数据
```python
class SnowballSentiment(BaseModel):
    symbol: str                           # 股票代码
    date: str                             # 日期
    positive_count: int                   # 正面情感数量
    negative_count: int                   # 负面情感数量
    neutral_count: int                    # 中性情感数量
    sentiment_score: float                # 情感得分
    discussion_count: int                 # 讨论数量
```

### 2. **EasyTrader 数据模型**

#### EasyTraderAccountInfo - 账户信息
```python
class EasyTraderAccountInfo(BaseModel):
    broker: str                           # 券商名称
    total_assets: float                   # 总资产
    available_cash: float                 # 可用资金
    market_value: float                   # 证券市值
    frozen_cash: float                    # 冻结资金
    total_profit_loss: float              # 总盈亏
    today_profit_loss: float              # 当日盈亏
```

#### EasyTraderPosition - 持仓信息
```python
class EasyTraderPosition(BaseModel):
    broker: str                           # 券商名称
    symbol: str                           # 股票代码
    name: str                             # 股票名称
    shares: int                           # 总股数
    available_shares: int                 # 可用股数
    cost_price: float                     # 成本价
    current_price: float                  # 当前价
    market_value: float                   # 市值
    profit_loss: float                    # 盈亏
    profit_loss_ratio: float              # 盈亏比例
```

#### EasyTraderTrade - 交易记录
```python
class EasyTraderTrade(BaseModel):
    broker: str                           # 券商名称
    trade_date: str                       # 交易日期
    symbol: str                           # 股票代码
    name: str                             # 股票名称
    trade_type: str                       # 交易类型
    shares: int                           # 成交股数
    price: float                          # 成交价格
    amount: float                         # 成交金额
    commission: float                     # 手续费
    stamp_duty: float                     # 印花税
```

## 🌐 **API 端点实现**

### 1. **Snowball API 端点**

#### 股票行情
```http
GET /rpc/snowball/quote?symbol=000001&market=cn
```

#### 财务数据
```http
GET /rpc/snowball/financial_data?symbol=000001&market=cn&period=annual
```

#### 研报数据
```http
GET /rpc/snowball/research_reports?symbol=000001&market=cn&limit=20
```

#### 情感分析
```http
GET /rpc/snowball/sentiment?symbol=000001&market=cn&days=7
```

#### 社区讨论
```http
GET /rpc/snowball/discussions?symbol=000001&market=cn&limit=50
```

#### 市场概览
```http
GET /rpc/snowball/market_overview?market=cn
```

### 2. **EasyTrader API 端点**

#### 账户登录
```http
POST /rpc/easytrader/login
{
    "username": "your_username",
    "password": "your_password",
    "broker": "ht",
    "exe_path": "/path/to/client.exe"
}
```

#### 账户信息
```http
GET /rpc/easytrader/account_info?broker=ht
```

#### 持仓信息
```http
GET /rpc/easytrader/portfolio?broker=ht
```

#### 交易记录
```http
GET /rpc/easytrader/trading_history?broker=ht&start_date=2024-01-01&end_date=2024-12-31
```

#### 市场数据
```http
GET /rpc/easytrader/market_data?broker=ht&symbols=000001,000002
```

#### 资金信息
```http
GET /rpc/easytrader/fund_info?broker=ht
```

#### 风险指标
```http
GET /rpc/easytrader/risk_metrics?broker=ht
```

## 🚀 **技术特性**

### 1. **异步支持**
- 所有适配器方法都使用 `async/await`
- 支持并发数据获取
- 基于 `aiolimiter` 的请求频率控制

### 2. **错误处理**
- 完善的异常捕获和处理
- 详细的日志记录
- 优雅的降级策略

### 3. **数据标准化**
- 统一的列名映射
- 跨数据源的格式一致性
- 自动数据类型转换

### 4. **限流控制**
- 集成 `aiolimiter` 进行请求频率控制
- 支持不同数据源的差异化限流策略
- 自动降级和重试机制

## 📈 **使用示例**

### 1. **Snowball 数据获取**
```python
import asyncio
from src.ak_unified.adapters.snowball_adapter import SnowballAdapter

async def get_snowball_data():
    adapter = SnowballAdapter()
    
    # 获取股票行情
    quote_df = await adapter.get_stock_quote('000001', 'cn')
    print(f"股票行情: {len(quote_df)} 条记录")
    
    # 获取研报数据
    reports_df = await adapter.get_stock_research_reports('000001', 'cn', 10)
    print(f"研报数据: {len(reports_df)} 条记录")
    
    # 获取情感分析
    sentiment_df = await adapter.get_stock_sentiment('000001', 'cn', 7)
    print(f"情感数据: {len(sentiment_df)} 条记录")

# 运行
asyncio.run(get_snowball_data())
```

### 2. **EasyTrader 数据获取**
```python
import asyncio
from src.ak_unified.adapters.easytrader_adapter import EasyTraderAdapter

async def get_easytrader_data():
    adapter = EasyTraderAdapter(broker='ht')
    
    # 登录账户
    success = await adapter.login('username', 'password')
    if not success:
        print("登录失败")
        return
    
    # 获取账户信息
    account_df = await adapter.get_account_info()
    print(f"账户信息: {len(account_df)} 条记录")
    
    # 获取持仓信息
    portfolio_df = await adapter.get_portfolio()
    print(f"持仓信息: {len(portfolio_df)} 条记录")
    
    # 获取交易记录
    trades_df = await adapter.get_trading_history('2024-01-01', '2024-12-31')
    print(f"交易记录: {len(trades_df)} 条记录")

# 运行
asyncio.run(get_easytrader_data())
```

### 3. **通过 API 端点获取**
```bash
# Snowball 数据
curl "http://localhost:8000/rpc/snowball/quote?symbol=000001&market=cn"
curl "http://localhost:8000/rpc/snowball/research_reports?symbol=000001&market=cn&limit=10"

# EasyTrader 数据
curl -X POST "http://localhost:8000/rpc/easytrader/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"your_username","password":"your_password","broker":"ht"}'

curl "http://localhost:8000/rpc/easytrader/portfolio?broker=ht"
```

## 🔮 **应用场景**

### 1. **Snowball 数据应用**
- **投资决策**: 基于研报和情感分析的投资决策
- **市场监控**: 实时监控股票讨论热度和情感变化
- **风险预警**: 通过情感数据识别潜在风险
- **竞争分析**: 分析竞争对手的社区关注度

### 2. **EasyTrader 数据应用**
- **账户管理**: 统一的券商账户管理平台
- **投资分析**: 基于实际持仓的投资组合分析
- **交易监控**: 实时监控交易活动和盈亏状况
- **风险控制**: 基于实际持仓的风险度量

## 📝 **依赖管理**

### 1. **新增依赖**
```toml
dependencies = [
    # ... 其他依赖
    "pysnowball>=0.0.0",
    "easytrader>=0.0.0"
]
```

### 2. **安装命令**
```bash
# 使用 uv
uv sync

# 或使用 pip
pip install pysnowball easytrader
```

## 🔮 **后续改进建议**

### 1. **数据质量提升**
- 添加数据验证和清洗逻辑
- 实现数据一致性检查
- 建立数据质量评分体系

### 2. **缓存优化**
- 实现 Redis 缓存策略
- 基于数据更新频率的动态缓存时间
- 缓存预热和失效策略

### 3. **实时更新**
- 添加 WebSocket 支持实时数据推送
- 实现事件驱动的数据更新
- 支持用户订阅和推送

### 4. **智能分析**
- 基于情感数据的市场情绪分析
- 研报数据的文本挖掘和分析
- 交易模式的智能识别

### 5. **用户功能**
- 个性化的数据推送
- 自定义的数据组合
- 多数据源的对比分析

## 📝 **总结**

本次扩展成功引入了两个重要的新数据源：

1. **Snowball 数据源**: 提供了丰富的社区数据、研报信息和情感分析，为投资决策提供了新的维度
2. **EasyTrader 数据源**: 实现了多券商的统一接入，为账户管理和投资分析提供了实际数据支持

这两个数据源的加入，使得 AK Unified 项目的数据覆盖更加全面，不仅包含了传统的市场数据，还涵盖了社区情感、机构研报和实际交易数据，为用户提供了更加丰富和实用的金融信息服务。

同时，新数据源的架构设计与现有系统保持一致，支持异步操作、限流控制和错误处理，确保了系统的稳定性和可扩展性。