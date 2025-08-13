# ak-unified

Unified interface and schemas for AkShare across macro, market, and securities categories. Managed by `uv`.

## Setup
```bash
uv venv
uv sync
uv run python -c "import ak_unified as aku; print(aku.__version__)"
```

## Structure
- `ak_unified/schemas`: Pydantic models for envelopes and domain schemas
- `ak_unified/registry.py`: Dataset registry mapping to AkShare functions
- `ak_unified/adapters/akshare_adapter.py`: Adapter to call AkShare and normalize outputs
- `ak_unified/dispatcher.py`: Unified entrypoints like `fetch_data`, `get_ohlcv`, etc.

## Notes
- Field names are normalized to snake_case and English.
- Timezone defaults to Asia/Shanghai unless otherwise specified.
- This project defines schemas and a unified interface over AkShare; real-time availability depends on upstream data sources.