# AK Unified 异步化改进

本文档描述了 AK Unified 项目的全面异步化改造，包括并发抓取、异步 HTTP 客户端、异步文件 I/O 等改进。

## 🚀 主要改进

### 1. 依赖更新

- **移除**: `requests` (同步 HTTP 客户端)
- **新增**: `aiohttp` (异步 HTTP 客户端), `aiofiles` (异步文件 I/O)

### 2. 异步化改造

#### 2.1 HTTP 客户端
- **Alpha Vantage 适配器**: 从 `urllib` 改为 `aiohttp`
- **所有 HTTP 请求**: 统一使用异步方式，避免阻塞事件循环

#### 2.2 文件 I/O
- **QMT 适配器**: 配置文件读取改为异步
- **工具文件**: `cache_tools.py`, `blob_tools.py` 改为异步
- **新增**: `async_file_tools.py` 提供完整的异步文件操作工具

#### 2.3 数据库连接
- **统一**: 所有 `get_pool()` 调用都使用 `await`
- **异步**: 数据库操作完全异步化

### 3. 并发批量抓取

#### 3.1 核心功能
```python
from ak_unified.dispatcher import fetch_data_batch

# 并发抓取多个数据集
tasks = [
    ("securities.equity.cn.ohlcv_daily", {"symbol": "000001.SZ"}),
    ("securities.equity.cn.ohlcv_daily", {"symbol": "000002.SZ"}),
    ("securities.equity.cn.quote", {})
]

results = await fetch_data_batch(
    tasks,
    max_concurrent=5,  # 最大并发数
    allow_fallback=True,
    use_cache=True
)
```

#### 3.2 便捷批量 API
```python
from ak_unified.dispatcher import get_ohlcv_batch, get_market_quotes_batch

# 批量获取 OHLCV 数据
ohlcv_results = await get_ohlcv_batch(
    symbols=["000001.SZ", "000002.SZ", "000858.SZ"],
    start="2024-01-01",
    end="2024-01-31",
    max_concurrent=3
)

# 批量获取市场报价
quote_results = await get_market_quotes_batch(
    symbols=["000001.SZ", "000002.SZ"],
    max_concurrent=2
)
```

#### 3.3 限流控制
```python
from ak_unified.dispatcher import fetch_data_with_rate_limiting

# 显式限流控制
env = await fetch_data_with_rate_limiting(
    "securities.equity.us.ohlcv_daily.av",
    {"symbol": "AAPL"},
    rate_limit_source="alphavantage"
)
```

### 4. 新的 API 端点

#### 4.1 批量抓取端点
```bash
# 通用批量抓取
POST /rpc/fetch_batch
{
    "tasks": [
        {"dataset_id": "securities.equity.cn.ohlcv_daily", "params": {"symbol": "000001.SZ"}},
        {"dataset_id": "securities.equity.cn.quote", "params": {}}
    ],
    "max_concurrent": 5
}

# 批量 OHLCV
POST /rpc/ohlcv_batch
{
    "symbols": ["000001.SZ", "000002.SZ"],
    "start": "2024-01-01",
    "end": "2024-01-31",
    "max_concurrent": 3
}

# 批量市场报价
POST /rpc/market_quotes_batch
{
    "symbols": ["000001.SZ", "000002.SZ"],
    "max_concurrent": 2
}

# 批量指数成分股
POST /rpc/index_constituents_batch
{
    "index_codes": ["000300.SH", "000905.SH"],
    "max_concurrent": 2
}
```

### 5. 异步文件工具

#### 5.1 基本操作
```python
from ak_unified.tools.async_file_tools import (
    read_text_file, write_text_file,
    read_json_file, write_json_file,
    read_csv_file, write_csv_file
)

# 异步读取文件
content = await read_text_file("config.json")
data = await read_json_file("data.json")
df = await read_csv_file("data.csv")

# 异步写入文件
await write_text_file("output.txt", "Hello World")
await write_json_file("output.json", {"key": "value"})
await write_csv_file("output.csv", df)
```

#### 5.2 批量操作
```python
from ak_unified.tools.async_file_tools import batch_read_files, batch_write_files

# 批量读取多个文件
file_paths = ["file1.txt", "file2.txt", "file3.txt"]
file_contents = await batch_read_files(file_paths)

# 批量写入多个文件
file_contents = {
    "output1.txt": "Content 1",
    "output2.txt": "Content 2"
}
write_results = await batch_write_files(file_contents)
```

#### 5.3 文件管理
```python
from ak_unified.tools.async_file_tools import (
    copy_file, move_file, delete_file,
    list_directory, create_directory
)

# 文件操作
await copy_file("source.txt", "dest.txt")
await move_file("old.txt", "new.txt")
await delete_file("temp.txt")

# 目录操作
await create_directory("new_folder")
files = await list_directory("data_folder", pattern="*.csv", recursive=True)
```

## 🔧 使用建议

### 1. 并发控制
- **小规模**: `max_concurrent=2-3` (适合限流严格的 API)
- **中等规模**: `max_concurrent=5-10` (平衡性能和资源)
- **大规模**: `max_concurrent=10-20` (需要足够的系统资源)

### 2. 错误处理
```python
try:
    results = await fetch_data_batch(tasks, max_concurrent=5)
    for i, result in enumerate(results):
        if hasattr(result, 'ak_function') and result.ak_function == 'error':
            print(f"Task {i} failed")
        else:
            print(f"Task {i} succeeded: {len(result.data)} records")
except Exception as e:
    print(f"Batch operation failed: {e}")
```

### 3. 性能优化
```python
# 使用缓存减少重复请求
results = await fetch_data_batch(
    tasks,
    use_cache=True,  # 启用缓存
    max_concurrent=5
)

# 批量操作时考虑内存使用
async def process_large_batch(symbols, batch_size=100):
    for i in range(0, len(symbols), batch_size):
        batch_symbols = symbols[i:i+batch_size]
        results = await get_ohlcv_batch(
            batch_symbols,
            max_concurrent=10
        )
        # 处理结果...
        await asyncio.sleep(0.1)  # 避免过度占用资源
```

## 📊 性能提升

### 1. 并发抓取
- **串行**: 100 个请求 × 200ms = 20 秒
- **并发 (10)**: 100 个请求 × 200ms ÷ 10 = 2 秒
- **提升**: **10x 性能提升**

### 2. 异步 I/O
- **同步文件操作**: 阻塞事件循环
- **异步文件操作**: 非阻塞，可并发处理
- **提升**: 更好的并发性能

### 3. 资源利用率
- **CPU**: 减少等待时间，提高利用率
- **内存**: 更好的内存管理
- **网络**: 并发连接，提高吞吐量

## 🚨 注意事项

### 1. 限流遵守
- 使用 `fetch_data_with_rate_limiting` 确保遵守 API 限流
- 合理设置 `max_concurrent` 避免触发限流

### 2. 错误处理
- 批量操作中的单个失败不会影响其他任务
- 检查返回结果的 `ak_function` 字段识别错误

### 3. 资源管理
- 大量并发请求可能消耗较多内存和网络连接
- 使用 `asyncio.Semaphore` 控制资源使用

### 4. 兼容性
- 所有现有 API 保持向后兼容
- 新增的异步功能不影响同步调用

## 🔮 未来计划

### 1. 更多异步适配器
- 将更多数据源适配器改为异步实现
- 支持 WebSocket 等实时数据源

### 2. 智能限流
- 基于 API 响应时间的动态限流调整
- 支持更复杂的限流策略

### 3. 缓存优化
- 异步缓存预热
- 智能缓存失效策略

### 4. 监控和指标
- 异步操作的性能指标收集
- 实时性能监控面板

## 📚 相关文档

- [API 参考文档](./API.md)
- [限流器使用指南](./RATE_LIMITING.md)
- [测试指南](./TESTING.md)
- [性能调优指南](./PERFORMANCE.md)