from __future__ import annotations

import os
from typing import Any, Dict, Optional

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()  # load .env if present
except Exception:
    pass


class Settings:
    # Logging
    LOG_LEVEL: str = os.getenv("AKU_LOG_LEVEL", "INFO")
    LOG_JSON: bool = os.getenv("AKU_LOG_JSON", "0") == "1"
    LOG_FORMAT: str = os.getenv("AKU_LOG_FORMAT", "{time} | {level} | {name}:{function}:{line} - {message}")

    # Database
    DB_DSN: Optional[str] = os.getenv("AKU_DB_DSN")
    CACHE_TTL_SECONDS: Optional[int] = int(os.getenv("AKU_CACHE_TTL_SECONDS", "0") or 0) or None
    CACHE_TTL_PER_DATASET: Optional[str] = os.getenv("AKU_CACHE_TTL_PER_DATASET")

    # Blob cache
    BLOB_ALLOW_PREFIXES: Optional[str] = os.getenv("AKU_BLOB_ALLOW_PREFIXES")
    BLOB_MAX_BYTES: Optional[int] = int(os.getenv("AKU_BLOB_MAX_BYTES", "0") or 0) or None
    BLOB_COMPRESS: bool = os.getenv("AKU_BLOB_COMPRESS", "0") == "1"

    # Rate limiting configuration
    RATE_LIMIT_ENABLED: bool = os.getenv("AKU_RATE_LIMIT_ENABLED", "1") == "1"
    
    # Alpha Vantage rate limits (requests per minute)
    AV_RATE_LIMIT_PER_MIN: int = int(os.getenv("AKU_AV_RATE_LIMIT_PER_MIN", "5"))
    AV_RATE_LIMIT_PER_DAY: int = int(os.getenv("AKU_AV_RATE_LIMIT_PER_DAY", "500"))
    
    # AkShare rate limits by vendor (requests per minute)
    AKSHARE_EASTMONEY_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_EASTMONEY_RATE_LIMIT", "60"))
    AKSHARE_SINA_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_SINA_RATE_LIMIT", "100"))
    AKSHARE_TENCENT_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_TENCENT_RATE_LIMIT", "80"))
    AKSHARE_THS_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_THS_RATE_LIMIT", "30"))
    AKSHARE_TDX_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_TDX_RATE_LIMIT", "50"))
    AKSHARE_BAIDU_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_BAIDU_RATE_LIMIT", "40"))
    AKSHARE_NETEASE_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_NETEASE_RATE_LIMIT", "60"))
    AKSHARE_HEXUN_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_HEXUN_RATE_LIMIT", "30"))
    AKSHARE_CSINDEX_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_CSINDEX_RATE_LIMIT", "20"))
    AKSHARE_JISILU_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_JISILU_RATE_LIMIT", "10"))
    AKSHARE_DEFAULT_RATE_LIMIT: int = int(os.getenv("AKU_AKSHARE_DEFAULT_RATE_LIMIT", "30"))
    
    # Snowball rate limits (requests per minute)
    SNOWBALL_DEFAULT_RATE_LIMIT: int = int(os.getenv("AKU_SNOWBALL_DEFAULT_RATE_LIMIT", "30"))
    
    # EasyTrader rate limits (requests per minute)
    EASYTRADER_DEFAULT_RATE_LIMIT: int = int(os.getenv("AKU_EASYTRADER_DEFAULT_RATE_LIMIT", "20"))
    EASYTRADER_LOGIN_RATE_LIMIT: int = int(os.getenv("AKU_EASYTRADER_LOGIN_RATE_LIMIT", "5"))

    # Vendors / adapters keys
    AV_API_KEY: Optional[str] = os.getenv("AKU_ALPHAVANTAGE_API_KEY") or os.getenv("ALPHAVANTAGE_API_KEY")
    IB_HOST: str = os.getenv("AKU_IB_HOST", "127.0.0.1")
    IB_PORT: int = int(os.getenv("AKU_IB_PORT", "7497"))
    IB_CLIENT_ID: int = int(os.getenv("AKU_IB_CLIENT_ID", "1"))

    # Region mapping
    REGION_MAPPING: Optional[str] = os.getenv("AKU_REGION_MAPPING")

    # Normalization rules override
    NORMALIZATION_RULES: Optional[str] = os.getenv("AKU_NORMALIZATION_RULES")


settings = Settings()