"""
Async Redis Client & Working Memory Cache Manager (Upstash Redis).
Provides sliding window message buffer, session caching, and rate limiting counters.
"""

import json
import logging
import uuid
from typing import Any, Dict, List, Optional
import redis.asyncio as aioredis
from app.core.config import settings

logger = logging.getLogger("redis_client")


class RedisService:
    """Singleton wrapper around async Redis client."""

    def __init__(self) -> None:
        self._redis: Optional[aioredis.Redis] = None

    @property
    def client(self) -> aioredis.Redis:
        """Lazily creates and returns the async Redis client."""
        if self._redis is None:
            self._redis = aioredis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                max_connections=20,
            )
        return self._redis

    async def close(self) -> None:
        """Closes the Redis connection pool."""
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    # =========================================================================
    # 1. WORKING MEMORY (Sliding Window Session Context)
    # =========================================================================
    def _working_memory_key(self, session_id: uuid.UUID) -> str:
        return f"session:{session_id}:working_memory"

    async def push_working_memory(
        self,
        session_id: uuid.UUID,
        sender: str,
        content: str,
        ttl_seconds: int = 7200,  # 24 Hours
    ) -> None:
        """
        Appends a conversation turn to the session's active working memory list.
        Maintains a sliding window with TTL expiration.
        """
        key = self._working_memory_key(session_id)
        item = json.dumps({"sender": sender, "content": content})
        await self.client.rpush(key, item)
        await self.client.expire(key, ttl_seconds)

    async def get_working_memory(
        self,
        session_id: uuid.UUID,
        limit: int = 10,
    ) -> List[Dict[str, str]]:
        """
        Retrieves the last `limit` messages from the active working memory buffer.
        """
        key = self._working_memory_key(session_id)
        raw_items = await self.client.lrange(key, -limit, -1)
        result: List[Dict[str, str]] = []
        for item in raw_items:
            try:
                result.append(json.loads(item))
            except Exception:
                continue
        return result

    async def clear_working_memory(self, session_id: uuid.UUID) -> None:
        """Deletes the active working memory for a session."""
        key = self._working_memory_key(session_id)
        await self.client.delete(key)

    # =========================================================================
    # 2. TENANT PROCEDURAL RULES CACHING
    # =========================================================================
    def _tenant_rules_key(self, tenant_id: uuid.UUID) -> str:
        return f"tenant:{tenant_id}:rules_cache"

    async def cache_tenant_rules(
        self,
        tenant_id: uuid.UUID,
        rules: Dict[str, Any],
        ttl_seconds: int = 3600,  # 1 Hour
    ) -> None:
        """Caches tenant procedural rules to avoid redundant PostgreSQL queries."""
        key = self._tenant_rules_key(tenant_id)
        await self.client.set(key, json.dumps(rules), ex=ttl_seconds)

    async def get_cached_tenant_rules(
        self,
        tenant_id: uuid.UUID,
    ) -> Optional[Dict[str, Any]]:
        """Fetches cached procedural rules for a tenant."""
        key = self._tenant_rules_key(tenant_id)
        raw = await self.client.get(key)
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                return None
        return None


redis_service = RedisService()


def get_redis_client() -> aioredis.Redis:
    """Helper function to obtain Redis client instance."""
    return redis_service.client
