# 个股事件日历与财报数据源调研报告

本文档调研了个股事件日历（包括财报发布和异动预告）以及财报数据获取渠道，为 AK Unified 项目提供数据源选择建议。

## 📅 事件日历数据源

### 1. 财报发布日历

#### 1.1 国内 A 股财报日历

**数据源：东方财富 (EastMoney)**
- **接口**: `akshare.stock_financial_report_em`
- **数据内容**: 
  - 报告期、报告类型（年报/中报/季报）
  - 公司代码、公司名称
  - 预约披露日期、实际披露日期
  - 每股收益、净利润等关键指标
- **更新频率**: 实时
- **覆盖范围**: A股全市场

**数据源：新浪财经 (Sina)**
- **接口**: `akshare.stock_financial_report_sina`
- **数据内容**: 财报预约披露时间表
- **特点**: 数据准确，更新及时

**数据源：巨潮资讯网**
- **接口**: `akshare.stock_financial_report_em`
- **数据内容**: 官方披露信息，权威性高
- **覆盖**: 深交所、上交所、北交所

#### 1.2 港股财报日历

**数据源：东方财富 (EastMoney)**
- **接口**: `akshare.stock_financial_hk_report_em`
- **数据内容**: 港股三大财务报表数据
- **特点**: 数据全面，更新及时

**数据源：百度财经**
- **接口**: `akshare.news_report_time_baidu`
- **数据内容**: 财报发行时间信息
- **特点**: 百度整理的财报时间表

#### 1.3 美股财报日历

**数据源：东方财富 (EastMoney)**
- **接口**: `akshare.stock_financial_us_report_em`
- **数据内容**: 美股三大财务报表数据
- **特点**: 数据标准化程度高

**数据源：Alpha Vantage**
- **接口**: `EARNINGS_CALENDAR`
- **数据内容**: 
  - 财报发布日期
  - 预期EPS、实际EPS
  - 预期营收、实际营收
- **限制**: 免费版有API调用限制

**数据源：Yahoo Finance**
- **接口**: `yfinance.Ticker.earnings_dates`
- **数据内容**: 历史财报日期、预期/实际数据
- **特点**: 免费，数据较全面

### 2. 异动预告日历

#### 2.1 业绩预告

**数据源：东方财富**
- **接口**: `akshare.stock_profit_forecast`
- **数据内容**: 
  - 预告类型（预增/预减/扭亏/续亏等）
  - 预告净利润变动幅度
  - 预告原因说明

**数据源：巨潮资讯网**
- **接口**: `akshare.stock_announcement_em`
- **数据内容**: 业绩预告公告全文
- **特点**: 官方公告，信息完整

#### 2.2 重大事项预告

**数据源：东方财富**
- **接口**: `akshare.stock_notice_report`
- **数据内容**: 
  - 重大事项类型
  - 预告日期、实施日期
  - 事项描述

**数据源：同花顺**
- **接口**: `akshare.stock_notice_report_ths`
- **数据内容**: 同花顺整理的异动预告
- **特点**: 分类清晰，便于筛选

## 📊 财报数据源

### 1. 财务指标数据

#### 1.1 基础财务指标

**数据源：东方财富 (A股)**
- **接口**: `akshare.stock_financial_analysis_indicator`
- **数据内容**: 
  - 盈利能力指标（ROE、ROA、毛利率等）
  - 成长能力指标（营收增长率、净利润增长率等）
  - 营运能力指标（存货周转率、应收账款周转率等）
  - 偿债能力指标（资产负债率、流动比率等）

**数据源：东方财富 (港股)**
- **接口**: `akshare.stock_financial_hk_analysis_indicator_em`
- **数据内容**: 港股财务分析主要指标
- **特点**: 港股专用，指标计算标准化

**数据源：东方财富 (美股)**
- **接口**: `akshare.stock_financial_us_analysis_indicator_em`
- **数据内容**: 美股财务分析主要指标
- **特点**: 美股专用，指标计算标准化

**数据源：新浪财经**
- **接口**: `akshare.stock_financial_analysis_indicator_sina`
- **数据内容**: 新浪整理的财务指标
- **特点**: 指标计算标准化

#### 1.2 财务报表数据

**数据源：巨潮资讯网 (A股)**
- **接口**: `akshare.stock_financial_report_em`
- **数据内容**: 
  - 资产负债表
  - 利润表
  - 现金流量表
- **特点**: 官方数据，权威性高

**数据源：东方财富 (港股)**
- **接口**: `akshare.stock_financial_hk_report_em`
- **数据内容**: 港股三大财务报表
- **特点**: 港股专用，数据全面

**数据源：东方财富 (美股)**
- **接口**: `akshare.stock_financial_us_report_em`
- **数据内容**: 美股三大财务报表
- **特点**: 美股专用，数据标准化

**数据源：东方财富 (A股)**
- **接口**: `akshare.stock_financial_analysis_indicator`
- **数据内容**: 三大报表的详细数据
- **特点**: 数据全面，更新及时

### 2. 美股财报数据

**数据源：Alpha Vantage**
- **接口**: `INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW`
- **数据内容**: 
  - 完整的财务报表
  - 历史数据（最多20年）
  - 标准化格式
- **限制**: 免费版有调用频率限制

**数据源：Yahoo Finance**
- **接口**: `yfinance.Ticker.financials`, `yfinance.Ticker.balance_sheet`
- **数据内容**: 财务报表数据
- **特点**: 免费，数据较新

## 🏦 基金持仓变动数据源

### 1. 基金持仓数据

**数据源：东方财富**
- **接口**: `akshare.fund_portfolio_hold_em`
- **数据内容**: 
  - 基金持仓股票明细
  - 持仓数量、持仓市值、占净值比例
  - 持仓变动情况
- **特点**: 数据全面，更新及时

**数据源：天天基金网**
- **接口**: `akshare.fund_portfolio_hold_em`
- **数据内容**: 基金持仓信息
- **特点**: 官方数据，权威性高

### 2. 基金重仓股数据

**数据源：东方财富**
- **接口**: `akshare.fund_portfolio_hold_em`
- **数据内容**: 
  - 基金重仓股票排名
  - 持仓比例、持仓市值
  - 持仓变动趋势
- **特点**: 便于分析基金投资偏好

### 3. 基金调仓数据

**数据源：东方财富**
- **接口**: `akshare.fund_portfolio_hold_em`
- **数据内容**: 
  - 基金调仓记录
  - 新增持仓、减仓股票
  - 调仓时间、调仓原因
- **特点**: 跟踪基金投资策略变化

## 🏗️ 技术实现方案

### 1. 数据获取架构

#### 1.1 分层架构设计
```
┌─────────────────────────────────────┐
│           API 层                    │
│  (FastAPI endpoints)               │
├─────────────────────────────────────┤
│           业务逻辑层                 │
│  (事件日历管理、财报分析)            │
├─────────────────────────────────────┤
│           数据适配层                 │
│  (AkShare, Alpha Vantage, etc.)    │
├─────────────────────────────────────┤
│           数据存储层                 │
│  (PostgreSQL + Redis缓存)          │
└─────────────────────────────────────┘
```

#### 1.2 数据流设计
```
数据源 → 适配器 → 数据清洗 → 标准化 → 存储 → 缓存 → API
```

### 2. 核心功能模块

#### 2.1 事件日历管理
- **财报日历**: 获取、更新、查询财报发布计划
- **异动预告**: 业绩预告、重大事项预告管理
- **提醒系统**: 重要事件临近提醒

#### 2.2 财报数据管理
- **财务指标**: 获取、计算、存储关键财务指标
- **报表数据**: 三大报表数据的获取和存储
- **数据分析**: 财务指标趋势分析、对比分析

#### 2.3 基金持仓管理
- **持仓数据**: 获取、更新、查询基金持仓信息
- **调仓分析**: 分析基金投资策略变化
- **重仓股跟踪**: 跟踪基金重仓股票变动

#### 2.4 数据同步策略
- **实时同步**: 重要公告实时获取
- **定时同步**: 定期更新财务数据
- **增量同步**: 只更新变化的数据

## 📋 具体实现建议

### 1. 新增数据源适配器

#### 1.1 财报日历适配器
```python
# src/ak_unified/adapters/earnings_calendar_adapter.py
class EarningsCalendarAdapter:
    async def get_earnings_calendar(self, market: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取财报日历"""
        pass
    
    async def get_earnings_dates(self, symbol: str) -> pd.DataFrame:
        """获取个股财报日期"""
        pass
```

#### 1.2 财务数据适配器
```python
# src/ak_unified/adapters/financial_data_adapter.py
class FinancialDataAdapter:
    async def get_financial_indicators(self, symbol: str, period: str) -> pd.DataFrame:
        """获取财务指标"""
        pass
    
    async def get_financial_statements(self, symbol: str, statement_type: str) -> pd.DataFrame:
        """获取财务报表"""
        pass
```

#### 1.3 基金持仓适配器
```python
# src/ak_unified/adapters/fund_portfolio_adapter.py
class FundPortfolioAdapter:
    async def get_fund_portfolio(self, fund_code: str) -> pd.DataFrame:
        """获取基金持仓"""
        pass
    
    async def get_fund_holdings_change(self, fund_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取基金持仓变动"""
        pass
```

### 2. 新增 API 端点

#### 2.1 事件日历端点
```python
@app.get("/rpc/earnings/calendar")
async def get_earnings_calendar(
    market: str = Query(...),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
) -> Dict[str, Any]:
    """获取财报日历"""
    pass

@app.get("/rpc/earnings/forecast")
async def get_earnings_forecast(
    symbol: str = Query(...),
    period: Optional[str] = Query(None)
) -> Dict[str, Any]:
    """获取业绩预告"""
    pass
```

#### 2.2 财务数据端点
```python
@app.get("/rpc/financial/indicators")
async def get_financial_indicators(
    symbol: str = Query(...),
    period: str = Query("annual")
) -> Dict[str, Any]:
    """获取财务指标"""
    pass

@app.get("/rpc/financial/statements")
async def get_financial_statements(
    symbol: str = Query(...),
    statement_type: str = Query(...),
    period: str = Query("annual")
) -> Dict[str, Any]:
    """获取财务报表"""
    pass
```

#### 2.3 基金持仓端点
```python
@app.get("/rpc/fund/portfolio")
async def get_fund_portfolio(
    fund_code: str = Query(...)
) -> Dict[str, Any]:
    """获取基金持仓"""
    pass

@app.get("/rpc/fund/holdings_change")
async def get_fund_holdings_change(
    fund_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...)
) -> Dict[str, Any]:
    """获取基金持仓变动"""
    pass
```

### 3. 数据模型设计

#### 3.1 事件日历模型
```python
# src/ak_unified/schemas/events.py
class EarningsEvent(BaseModel):
    symbol: str
    company_name: Optional[str]
    report_period: str
    report_type: str  # annual, semi_annual, quarterly
    scheduled_date: Optional[datetime]
    actual_date: Optional[datetime]
    eps_estimate: Optional[float]
    eps_actual: Optional[float]
    revenue_estimate: Optional[float]
    revenue_actual: Optional[float]
    source: str
    market: str
    created_at: datetime
    updated_at: datetime

class EarningsForecast(BaseModel):
    symbol: str
    forecast_period: str
    forecast_type: str  # pre_increase, pre_decrease, turn_profit, etc.
    net_profit_change: Optional[float]
    change_reason: Optional[str]
    announcement_date: datetime
    source: str
    market: str
```

#### 3.2 财务数据模型
```python
# src/ak_unified/schemas/financial.py
class FinancialIndicator(BaseModel):
    symbol: str
    period: str
    indicator_name: str
    indicator_value: float
    unit: Optional[str]
    source: str
    market: str
    report_date: datetime

class FinancialStatement(BaseModel):
    symbol: str
    statement_type: str  # balance_sheet, income_statement, cash_flow
    period: str
    report_date: datetime
    data: Dict[str, Any]  # 具体的财务数据
    source: str
    market: str
```

#### 3.3 基金持仓模型
```python
# src/ak_unified/schemas/fund.py
class FundPortfolio(BaseModel):
    fund_code: str
    fund_name: str
    report_date: datetime
    total_assets: Optional[float]
    stock_holdings: List[StockHolding]
    source: str
    created_at: datetime

class StockHolding(BaseModel):
    symbol: str
    stock_name: str
    shares: int
    market_value: float
    percentage: float
    change_shares: Optional[int]
    change_percentage: Optional[float]
```

## 🚀 实施优先级

### 1. 第一阶段（核心功能）
- [ ] 财报日历数据获取（东方财富 + 巨潮资讯网）
- [ ] 基础财务指标获取
- [ ] 核心 API 端点实现

### 2. 第二阶段（扩展功能）
- [ ] 业绩预告数据获取
- [ ] 港股和美股财报数据
- [ ] 基金持仓数据获取

### 3. 第三阶段（高级功能）
- [ ] 智能提醒系统
- [ ] 财务指标趋势分析
- [ ] 基金调仓分析
- [ ] 多数据源数据融合

## 📊 数据质量评估

### 1. 数据准确性
- **东方财富**: ⭐⭐⭐⭐⭐ (官方数据，准确性高)
- **巨潮资讯网**: ⭐⭐⭐⭐⭐ (官方披露，权威性最高)
- **新浪财经**: ⭐⭐⭐⭐ (数据整理规范)
- **百度财经**: ⭐⭐⭐⭐ (财报时间信息准确)
- **Alpha Vantage**: ⭐⭐⭐⭐ (数据标准化程度高)
- **Yahoo Finance**: ⭐⭐⭐ (免费数据，质量一般)

### 2. 数据完整性
- **东方财富**: ⭐⭐⭐⭐⭐ (覆盖全面)
- **巨潮资讯网**: ⭐⭐⭐⭐⭐ (官方要求，完整性最高)
- **新浪财经**: ⭐⭐⭐⭐ (主要指标覆盖)
- **百度财经**: ⭐⭐⭐⭐ (财报时间覆盖全面)
- **Alpha Vantage**: ⭐⭐⭐⭐ (美股数据完整)
- **Yahoo Finance**: ⭐⭐⭐ (基础数据完整)

### 3. 更新及时性
- **东方财富**: ⭐⭐⭐⭐⭐ (实时更新)
- **巨潮资讯网**: ⭐⭐⭐⭐⭐ (官方实时披露)
- **新浪财经**: ⭐⭐⭐⭐ (更新及时)
- **百度财经**: ⭐⭐⭐⭐ (财报时间更新及时)
- **Alpha Vantage**: ⭐⭐⭐⭐ (API 实时)
- **Yahoo Finance**: ⭐⭐⭐ (有一定延迟)

## 💡 技术建议

### 1. 数据源选择策略
- **主要数据源**: 东方财富 + 巨潮资讯网（国内）
- **辅助数据源**: 百度财经（财报时间）、Alpha Vantage + Yahoo Finance（美股）
- **备用数据源**: 新浪财经、同花顺等

### 2. 缓存策略
- **实时数据**: 财报日历、重要公告（缓存时间短）
- **历史数据**: 财务报表、财务指标（缓存时间长）
- **缓存更新**: 基于数据源更新频率动态调整

### 3. 错误处理
- **重试机制**: 网络请求失败自动重试
- **降级策略**: 主要数据源失败时使用备用数据源
- **数据验证**: 获取数据后进行格式和逻辑验证

## 📝 总结

通过调研，我们确定了以下关键数据源和实现方案：

1. **事件日历**: 以东方财富、巨潮资讯网和百度财经为主要数据源
2. **财报数据**: 结合多个数据源，确保数据完整性和准确性
3. **基金持仓**: 以东方财富为主要数据源，获取基金持仓和调仓信息
4. **技术架构**: 采用分层设计，支持多数据源适配
5. **实施路径**: 分阶段实施，优先实现核心功能

这个方案将为 AK Unified 项目提供全面的个股事件日历、财报数据和基金持仓信息支持，满足用户对财务信息的全面需求。