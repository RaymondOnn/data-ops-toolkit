import logging
from collections.abc import Generator
from typing import Any

from .base import PriorityQueue, TaskMessage

LOG = logging.getLogger(__name__)


class RedisPriorityQueue(PriorityQueue):
    """Redis implementation using Sorted Sets for priority."""

    def __init__(
        self, redis_url: str = "redis://localhost:6379/0", queue_key: str = "task_queue"
    ):
        import redis

        self.redis = redis.from_url(redis_url)
        self.queue_key = queue_key
        self.processing_key = f"{queue_key}:processing"
        LOG.info(f"RedisQueue initialized | URL={redis_url}")

    def push(
        self,
        data: Any,
        priority: int,
        group: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Push using Sorted Set (score = priority)."""
        import json
        import uuid

        msg_id = str(uuid.uuid4())
        payload = {
            "id": msg_id,
            "data": data,
            "metadata": metadata or {},
            "group": group,
        }

        # Sorted set with priority as score (lower = higher priority)
        self.redis.zadd(self.queue_key, {json.dumps(payload): priority})

    def pop(self, visibility_timeout: int = 300) -> TaskMessage | None:
        """Pop highest priority using atomic Lua script."""
        import json

        # Atomic pop: get highest priority, move to processing
        lua_script = """
        local queue_key = KEYS[1]
        local processing_key = KEYS[2]
        local timeout = ARGV[1]

        -- Get highest priority item
        local items = redis.call('ZRANGE', queue_key, 0, 0)
        if #items == 0 then
            return nil
        end

        local item = items[1]
        redis.call('ZREM', queue_key, item)
        redis.call('SETEX', processing_key .. ':' .. item, timeout, item)
        return item
        """

        result = self.redis.eval(
            lua_script, 2, self.queue_key, self.processing_key, visibility_timeout
        )

        if not result:
            return None

        # Narrow the type to satisfy the checker and ensure runtime compatibility with json.loads
        if not isinstance(result, str | bytes | bytearray):
            return None

        payload = json.loads(result)
        return TaskMessage(
            id_=payload["id"], data=payload["data"], metadata=payload["metadata"]
        )

    def ack(self, msg_id: str) -> None:
        """Acknowledge and remove from processing."""
        self.redis.delete(f"{self.processing_key}:{msg_id}")

    def size(self) -> int:
        """Return queue size."""
        result = self.redis.zcard(self.queue_key)
        if isinstance(result, int):
            return result
        return 0

    def items(self) -> Generator[Any, None, None]:
        """Scan and yield data from all items in the Redis sorted set."""
        import json

        try:
            # Fetch all raw elements from the sorted set
            items = self.redis.zrange(self.queue_key, 0, -1)

            # Explicit guard to satisfy type checkers that 'items' is a list, not an Awaitable
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, (bytes | bytearray)):
                        payload_str = item.decode("utf-8")
                    elif isinstance(item, str):
                        payload_str = item
                    else:
                        # Fallback/Cast to ensure standard type stringification
                        payload_str = str(item)

                    payload = json.loads(payload_str)
                    yield payload["data"]
        except Exception as e:
            LOG.error(f"Failed to scan Redis queue items: {e}")

    def cleanup(self) -> None:
        """Clean up expired processing items."""
        # Redis TTL handles this automatically
        pass

    def shutdown(self) -> None:
        """Shutdown Redis connection."""
        self.redis.close()
