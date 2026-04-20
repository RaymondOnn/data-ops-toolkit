
from pathlib import Path
from typing import Any

from .base import KeyValueCache
from .diskcache import DiskCache
from .redis import RedisCache


def get_cache(workspace_dir: Path, cache_cfg: dict[str, Any]) -> KeyValueCache:
    """
    Standalone factory to create a normalized CacheService.
    Infrastructure-level: does not depend on Core or Services.
    """
    if cache_cfg.get("type") == "redis":
        return RedisCache(
            host=cache_cfg.get("host", "localhost"),
            port=cache_cfg.get("port", 6379),
            db=cache_cfg.get("db", 0)
        )

    # Default to lean mode (Diskcache)
    cache_filepath = cache_cfg.get("filepath", ".cache")
    return DiskCache(
        cache_path=(workspace_dir / cache_filepath).resolve()
    )
