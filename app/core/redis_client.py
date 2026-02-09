"""Shared async Redis client."""

from __future__ import annotations

import redis.asyncio as aioredis

from app.core.config import REDIS_URL

_pool: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    """Return (and lazily create) the shared async Redis connection pool."""
    global _pool
    if _pool is None:
        _pool = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _pool


async def close_redis() -> None:
    """Gracefully close the Redis connection pool."""
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None
