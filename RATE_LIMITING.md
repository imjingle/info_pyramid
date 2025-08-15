# Rate Limiting 限速功能说明

## 概述

AK Unified 引入了基于 `aiolimiter` 的请求限速系统，用于控制不同数据源的请求频度，避免超过 API 限速后拒绝服务。

## 支持的限速策略

### 1. Alpha Vantage
- **每分钟限速**: 默认 5 请求/分钟（免费版）
- **每日限速**: 默认 500 请求/天（免费版）
- **付费版**: 可配置更高的限制

### 2. AkShare 各数据源
根据不同的数据供应商设置不同的限速策略：

| 供应商 | 默认限速 (请求/分钟) | 说明 |
|--------|---------------------|------|
| 东方财富 (eastmoney) | 60 | 主要数据源，相对宽松 |
| 新浪财经 (sina) | 100 | 实时数据，较高频率 |
| 腾讯财经 (tencent) | 80 | 综合数据源 |
| 同花顺 (ths) | 30 | 专业数据，较严格 |
| 通达信 (tdx) | 50 | 技术分析数据 |
| 百度财经 (baidu) | 40 | 新闻资讯数据 |
| 网易财经 (netease) | 60 | 财经新闻数据 |
| 和讯财经 (hexun) | 30 | 专业财经数据 |
| 中证指数 (csindex) | 20 | 指数数据，严格限制 |
| 集思录 (jisilu) | 10 | 债券数据，最严格 |
| 默认 (default) | 30 | 未知供应商的默认限制 |

## 配置方法

### 环境变量配置

在 `.env` 文件中设置以下变量：

```bash
# 启用限速功能
AKU_RATE_LIMIT_ENABLED=1

# Alpha Vantage 限速
AKU_AV_RATE_LIMIT_PER_MIN=5
AKU_AV_RATE_LIMIT_PER_DAY=500

# AkShare 各供应商限速
AKU_AKSHARE_EASTMONEY_RATE_LIMIT=60
AKU_AKSHARE_SINA_RATE_LIMIT=100
AKU_AKSHARE_TENCENT_RATE_LIMIT=80
AKU_AKSHARE_THS_RATE_LIMIT=30
AKU_AKSHARE_TDX_RATE_LIMIT=50
AKU_AKSHARE_BAIDU_RATE_LIMIT=40
AKU_AKSHARE_NETEASE_RATE_LIMIT=60
AKU_AKSHARE_HEXUN_RATE_LIMIT=30
AKU_AKSHARE_CSINDEX_RATE_LIMIT=20
AKU_AKSHARE_JISILU_RATE_LIMIT=10
AKU_AKSHARE_DEFAULT_RATE_LIMIT=30
```

### 动态配置

限速器支持运行时动态调整，但需要重启服务才能生效。

## 使用方法

### 1. 自动限速

限速功能默认自动启用，所有通过适配器的请求都会自动应用相应的限速策略：

```python
# 使用 Alpha Vantage 适配器
from ak_unified.adapters.alphavantage_adapter import call_alphavantage

# 自动应用限速
result = await call_alphavantage("securities.equity.us.ohlcv_daily.av", {"symbol": "AAPL"})
```

```python
# 使用 AkShare 适配器
from ak_unified.adapters.akshare_adapter import call_akshare

# 自动识别供应商并应用限速
result = await call_akshare(["stock_zh_a_hist"], {"symbol": "000001"})
```

### 2. 手动限速控制

```python
from ak_unified.rate_limiter import acquire_rate_limit, acquire_daily_rate_limit

# 获取 Alpha Vantage 限速许可
await acquire_rate_limit('alphavantage')
await acquire_daily_rate_limit('alphavantage')

# 获取 AkShare 特定供应商限速许可
await acquire_rate_limit('akshare', 'eastmoney')
```

### 3. 查看限速状态

```python
from ak_unified.rate_limiter import get_rate_limit_status

# 获取所有限速器状态
status = await get_rate_limit_status()
print(status)
```

## API 端点

### 查看限速状态

```http
GET /rpc/rate-limits
```

响应示例：
```json
{
  "rate_limiting_enabled": true,
  "limiters": {
    "alphavantage": {
      "max_rate": 5,
      "time_period": 60.0,
      "available_tokens": 3,
      "last_refill": 1703123456.789
    },
    "alphavantage_daily": {
      "max_rate": 500,
      "time_period": 86400.0,
      "available_tokens": 450,
      "last_refill": 1703123456.789
    },
    "akshare_eastmoney": {
      "max_rate": 60,
      "time_period": 60.0,
      "available_tokens": 45,
      "last_refill": 1703123456.789
    }
  }
}
```

## 限速算法

使用 **Token Bucket** 算法实现限速：

1. **令牌桶**: 每个限速器维护一个令牌桶
2. **令牌生成**: 按照配置的速率持续生成令牌
3. **请求消耗**: 每个请求消耗一个令牌
4. **限速控制**: 当令牌不足时，请求会被阻塞直到有可用令牌

## 最佳实践

### 1. 合理设置限速

- **Alpha Vantage**: 根据订阅计划调整限制
- **AkShare**: 根据实际使用情况和数据源稳定性调整
- **监控**: 定期检查限速状态，避免过度限制

### 2. 错误处理

```python
try:
    result = await call_alphavantage(dataset_id, params)
except Exception as e:
    if "rate limit" in str(e).lower():
        # 处理限速错误
        await asyncio.sleep(60)  # 等待1分钟
        result = await call_alphavantage(dataset_id, params)
```

### 3. 批量请求优化

对于批量请求，建议：
- 使用适当的并发控制
- 实现重试机制
- 监控限速状态

## 故障排除

### 1. 限速不生效

检查：
- `AKU_RATE_LIMIT_ENABLED` 是否设置为 `1`
- 环境变量是否正确加载
- 日志中是否有限速相关错误

### 2. 请求被过度阻塞

可能原因：
- 限速值设置过低
- 并发请求过多
- 令牌桶配置不当

### 3. 性能问题

如果限速影响性能：
- 调整限速值
- 使用缓存减少请求
- 实现请求队列

## 监控和日志

限速器会记录详细的日志信息：

```
2024-01-01 12:00:00 | INFO | rate_limiter:acquire:45 - Rate limit acquired for alphavantage
2024-01-01 12:00:01 | WARNING | rate_limiter:acquire:52 - Failed to acquire rate limit for akshare_eastmoney: Rate limit exceeded
```

## 扩展性

限速系统设计为可扩展的：

1. **新增数据源**: 在 `RateLimiterManager` 中添加新的限速器
2. **自定义策略**: 实现自定义的限速算法
3. **动态配置**: 支持运行时配置更新
4. **监控集成**: 与 Prometheus、Grafana 等监控系统集成
