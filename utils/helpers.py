import os
import random
import asyncio
from datetime import datetime
from typing import List, Dict, Any


def get_timestamp() -> int:
    return int(datetime.now().timestamp())


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def format_time(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def safe_str(value: Any, default: str = '') -> str:
    if value is None or value == '' or str(value).lower() in ('nan', 'none', 'null'):
        return default
    return str(value).strip().replace('\n', ' ').replace('\r', ' ')


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def jitter_delay(base: float, ratio: float = 0.3) -> float:
    """返回 base ± ratio 的随机延迟，模拟人类行为。
    
    例: base=1.0, ratio=0.3 → 返回 0.7~1.3
    """
    return base + random.uniform(-base * ratio, base * ratio)


async def sleep_jitter(base: float, ratio: float = 0.3):
    """随机抖动等待，替代 asyncio.sleep(delay)。"""
    if base is None:
        base = 1.0
    await asyncio.sleep(jitter_delay(base, ratio))
