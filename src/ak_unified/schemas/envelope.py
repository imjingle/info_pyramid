from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class Pagination(BaseModel):
    offset: int = 0
    limit: int = 0
    total: Optional[int] = None


class DataEnvelope(BaseModel):
    schema_version: str = Field(default="1.0.0")
    provider: str = Field(default="akshare")
    category: str
    domain: str
    dataset: str
    params: Dict[str, Any] = Field(default_factory=dict)
    timezone: str = Field(default="Asia/Shanghai")
    currency: Optional[str] = None
    attribution: str = Field(
        default=(
            "Data via AkShare (https://akshare.akfamily.xyz) and respective public sources"
        )
    )
    data: List[Dict[str, Any]] = Field(default_factory=list)
    pagination: Pagination = Field(default_factory=Pagination)