# 依赖修复总结

本文档记录了修复 AK Unified 项目依赖问题的过程。

## 🚨 发现的问题

### 1. 缺失的依赖
- **`sse-starlette`**: API 模块中使用了 `EventSourceResponse`，但 `pyproject.toml` 中没有相应依赖
- **`BaseAdapterError`**: 适配器模块中引用了不存在的基类

### 2. 语法错误
- **异步生成器中的 `return` 语句**: 在异步生成器函数中使用了 `return` 而不是 `yield`
- **同步函数中的 `await`**: 在同步函数中使用了 `await` 关键字

### 3. FastAPI 路由问题
- **复杂类型查询参数**: 尝试将 `Dict[str, Any]` 作为查询参数，FastAPI 不支持

## 🔧 修复过程

### 1. 添加缺失依赖

#### 1.1 更新 `pyproject.toml`
```toml
dependencies = [
    # ... 其他依赖 ...
    "sse-starlette>=1.8.2"  # 新增
]
```

#### 1.2 安装依赖
```bash
uv sync
```

### 2. 创建缺失的基类

#### 2.1 创建 `src/ak_unified/adapters/base.py`
```python
class BaseAdapterError(Exception):
    """Base exception class for all adapter errors."""
    
    def __init__(self, message: str, *args, **kwargs):
        super().__init__(message, *args, **kwargs)
        self.message = message
```

#### 2.2 添加其他异常类
- `AdapterConnectionError`
- `AdapterAuthenticationError`
- `AdapterRateLimitError`
- `AdapterDataError`
- `AdapterTimeoutError`
- `AdapterNotSupportedError`

### 3. 修复语法错误

#### 3.1 异步生成器中的 `return` 语句
**修复前:**
```python
async def gen():
    # ...
    if not q.empty:
        return q  # ❌ 错误：异步生成器中不能使用 return
```

**修复后:**
```python
async def gen():
    # ...
    if not q.empty:
        yield {"event": "update", "data": q.to_dict(orient='records')}  # ✅ 正确
        continue
```

#### 3.2 同步函数中的 `await`
**修复前:**
```python
def fetch_cons_one(b: str) -> _pd.DataFrame:
    # ...
    env = await fetch_data(ds, {"board_code": b})  # ❌ 错误：同步函数中不能使用 await
```

**修复后:**
```python
async def fetch_cons_one(b: str) -> _pd.DataFrame:  # ✅ 改为异步函数
    # ...
    env = await fetch_data(ds, {"board_code": b})
```

### 4. 修复 FastAPI 路由问题

#### 4.1 复杂类型查询参数
**修复前:**
```python
@app.get("/admin/cache/blob")
async def admin_cache_blob_get(
    dataset_id: str = Query(...),
    params: Dict[str, Any] = Query(...)  # ❌ 错误：FastAPI 不支持复杂类型查询参数
) -> Dict[str, Any]:
```

**修复后:**
```python
@app.get("/admin/cache/blob")
async def admin_cache_blob_get(
    dataset_id: str = Query(...),
    symbol: Optional[str] = Query(None),  # ✅ 正确：使用简单类型
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
) -> Dict[str, Any]:
    # 在函数内部构建参数字典
    params = {}
    if symbol:
        params["symbol"] = symbol
    if start:
        params["start"] = start
    if end:
        params["end"] = end
```

#### 4.2 修复的端点
- `/admin/cache/blob` - 改为使用简单查询参数
- `/admin/cache/blob/purge` - 移除复杂参数
- `/rpc/replay` - 改为使用简单查询参数
- `/rpc/stream` - 改为使用简单查询参数

### 5. 添加缺失的导入

#### 5.1 添加 pandas 导入
```python
import pandas as pd  # 新增：用于时间戳生成
```

## ✅ 修复结果

### 1. 依赖完整性
- ✅ `sse-starlette` 依赖已添加
- ✅ `BaseAdapterError` 基类已创建
- ✅ 所有适配器异常类已定义

### 2. 语法正确性
- ✅ 异步生成器使用 `yield` 而不是 `return`
- ✅ 所有异步函数正确使用 `async def`
- ✅ 所有 `await` 调用都在异步函数中

### 3. FastAPI 兼容性
- ✅ 所有端点使用简单类型查询参数
- ✅ 复杂参数在函数内部构建
- ✅ 路由定义语法正确

### 4. 模块导入
- ✅ API 模块可以正常导入
- ✅ 所有依赖关系正确解析

## 🧪 验证步骤

### 1. 依赖安装验证
```bash
uv sync
python -c "from sse_starlette.sse import EventSourceResponse; print('sse-starlette import successful')"
```

### 2. 基类导入验证
```bash
python -c "from src.ak_unified.adapters.base import BaseAdapterError; print('BaseAdapterError import successful')"
```

### 3. API 模块导入验证
```bash
python -c "import src.ak_unified.api; print('API module import successful')"
```

## 📚 经验总结

### 1. 依赖管理
- 始终检查代码中使用的所有外部库
- 在 `pyproject.toml` 中明确声明所有依赖
- 使用 `uv sync` 确保依赖版本一致

### 2. 异步编程
- 异步生成器函数只能使用 `yield`，不能使用 `return`
- 使用 `await` 的函数必须声明为 `async def`
- 注意异步函数的调用链

### 3. FastAPI 最佳实践
- 查询参数只支持简单类型（str, int, bool 等）
- 复杂类型参数使用 `Body()` 或 `Form()`
- 在函数内部构建复杂数据结构

### 4. 错误处理
- 创建统一的异常基类
- 为不同类型的错误定义专门的异常类
- 提供有意义的错误消息

## 🔮 后续改进

### 1. 依赖检查
- 添加依赖完整性检查脚本
- 在 CI/CD 中验证所有导入的模块都有对应依赖

### 2. 代码质量
- 添加类型检查工具（mypy）
- 添加代码格式化工具（black, isort）
- 添加代码质量检查工具（flake8）

### 3. 测试覆盖
- 为所有修复的功能添加单元测试
- 验证异步函数的正确性
- 测试 FastAPI 端点的参数处理

## 📝 相关文件

- `pyproject.toml` - 项目依赖配置
- `src/ak_unified/adapters/base.py` - 适配器基类定义
- `src/ak_unified/api.py` - API 端点定义（已修复）
- `ASYNC_IMPROVEMENTS.md` - 异步化改进文档