# libs/storage/cache/factory.py
import logging
from enum import StrEnum

from .base import Cache

LOG = logging.getLogger(__name__)


class CacheType(StrEnum):
    DISKCACHE = "diskcache"
    REDIS = "redis"
    MEMORY = "memory"


class CacheFactory:
    """Simple factory for creating cache instances."""

    @staticmethod
    def create(
        **kwargs,
    ) -> Cache:
        """Create a cache instance.

        Args:
            cache_type: The type of cache to create.
            namespace: The namespace for the cache.
            **kwargs: Additional arguments for the cache.

        Returns:
            Cache: A cache instance.

        Raises:
            ValueError: If the cache type is not supported or if required arguments are missing.

        Examples:
            # DiskCache (persistent, no eviction)
            cache = CacheFactory.create(
                CacheType.DISKCACHE,
                namespace="ingestion",
                directory=Path("/path/to/cache"),
                evict=False
            )

            # DiskCache (with eviction and TTL)
            cache = CacheFactory.create(
                CacheType.DISKCACHE,
                namespace="cache",
                directory=Path("/path/to/cache"),
                evict=True,
                size_limit=2**30
            )

            # Redis
            cache = CacheFactory.create(
                CacheType.REDIS,
                namespace="ingestion",
                redis_url="redis://localhost:6379/0"
            )

            # Memory (for testing)
            cache = CacheFactory.create(
                CacheType.MEMORY,
                namespace="test"
            )
        """
        key = kwargs.get("key")
        if not key:
            raise ValueError("Cache Key is required...")

        namespace = kwargs.get("namespace")
        match key:
            case CacheType.DISKCACHE:
                from .diskcache import DiskCache

                if not (directory := kwargs.get("directory")):
                    raise ValueError("directory is required for DiskCache")

                cache: Cache = DiskCache(
                    directory=directory,
                    namespace=namespace,
                    size_limit=kwargs.get("size_limit", 2**30),
                    timeout=kwargs.get("timeout", 5),
                )

            case CacheType.REDIS:
                from .redis import RedisCache

                cache = RedisCache(
                    redis_url=kwargs.get("redis_url", "redis://localhost:6379/0"),
                    namespace=namespace,
                    **{k: v for k, v in kwargs.items() if k not in ["redis_url"]},
                )

            case CacheType.MEMORY:
                from .memory import MemoryCache

                cache: Cache = MemoryCache(namespace=namespace)

            case _:
                raise ValueError(f"Unsupported cache type: {key}")

        LOG.info(f"Created {key} cache | namespace={namespace}")
        return cache
