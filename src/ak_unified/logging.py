from __future__ import annotations

import os
import sys
from typing import Optional
from loguru import logger


def _default_level() -> str:
    return os.getenv("AKU_LOG_LEVEL", "INFO")


def _default_format() -> str:
    # structured-ish line; users can set AKU_LOG_JSON=1 for JSON sink
    return os.getenv("AKU_LOG_FORMAT", "{time} | {level} | {name}:{function}:{line} - {message}")


def configure_logger(json_output: Optional[bool] = None) -> None:
    logger.remove()
    level = _default_level()
    fmt = _default_format()
    json_enabled = json_output if json_output is not None else (os.getenv("AKU_LOG_JSON", "0") == "1")
    if json_enabled:
        def serialize(record):
            from json import dumps
            payload = {
                "time": record["time"].isoformat(),
                "level": record["level"].name,
                "message": record["message"],
                "name": record["name"],
                "function": record["function"],
                "line": record["line"],
                "extra": record.get("extra", {}),
            }
            return dumps(payload) + "\n"
        logger.add(sys.stdout, level=level, serialize=False, format=serialize, enqueue=True, backtrace=False, diagnose=False)
    else:
        logger.add(sys.stdout, level=level, format=fmt, enqueue=True, backtrace=False, diagnose=False)


# initialize on import for library default
try:
    configure_logger()
except Exception:
    pass