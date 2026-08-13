import time
from itertools import takewhile
from typing import Any

import msgspec
from libs.storage.cache import CacheFactory
from libs.utils.dates import current_timestamp
from loguru import logger

from src.core.models.task.enums import ExecutionStatus
from src.core.orchestrator.common.state import StateHub
from src.core.orchestrator.enums import TaskMetadata

LOG = logger


class TaskCache:
    """Manages the atomic lifecycles of active cache entries and external persistent state."""

    def __init__(self, cache_config, prefix: str, state_hub: StateHub | None = None):
        self.prefix = f"{prefix.rstrip(':')}:" if prefix else prefix
        self.client = CacheFactory.create(
            key=cache_config["key"],
            directory=str(cache_config["directory"]),
            namespace=prefix,
            size_limit=cache_config.get("size_limit", 2**30),
            timeout=cache_config.get("timeout", 5),
        )
        self.state_hub = state_hub  # Your external DB / State Store instance
        LOG.trace(
            f"TRACE:TaskCache initialised. Cache Location: {self.client.directory}"
        )
        LOG.debug(
            f"DEBUG:TaskCache initialised. Cache Location: {self.client.directory}"
        )
        LOG.info(f"INFO:TaskCache initialised. Cache Location: {self.client.directory}")

    def _sanitize_key(self, key: str) -> str:
        """Robust key sanitizer.

        - If the key has exactly one prefix, leaves it as-is.
        - If it has 0 or >1 prefixes, strips them all and forces exactly one.
        """
        prefix_word = self.prefix.rstrip(":")

        # Split by colon to inspect the structure
        parts = key.split(":")

        # Count how many consecutive times the prefix_name appears at the start
        prefix_count = sum(1 for part in takewhile(lambda p: p == prefix_word, parts))

        # Case 1: The key is already perfectly formatted with exactly one prefix
        if prefix_count == 1:
            return key

        # Case 2: The key has no prefix or has multiple duplicate prefixes
        # Slice away all the duplicate prefix segments and re-join the rest
        clean_body = ":".join(parts[prefix_count:])

        # Re-attach exactly one prefix to the clean body string
        return f"{self.prefix}{clean_body}"

    def get(self, key: str) -> TaskMetadata:
        """Retrieves and strongly types metadata from cache with an automatic database fallback."""
        cache_key = self._sanitize_key(key)

        client_key = cache_key
        client_key = client_key.removeprefix(self.prefix)

        try:
            raw = self.client.get(client_key)
            if raw:
                if isinstance(raw, TaskMetadata):
                    return raw

                # Otherwise, fall back to parsing raw bytes/strings
                return TaskMetadata.from_raw_cache(raw)
        except Exception as err:
            LOG.exception(
                f"Serialization crash decoding metadata inside worker for key {cache_key}: {err}"
            )
            raise err

        # Fallback Strategy: If cache evaporated but worker is recovering, check State Store
        run_id = cache_key.rsplit(":", maxsplit=1)[
            -1
        ]  # Safely localized inside repository boundaries
        if self.state_hub:
            db_record = self.state_hub.store.get(run_id)
            if db_record:
                metadata = msgspec.convert(db_record, type=TaskMetadata)
                # Rehydrate active cache
                self.client.set(cache_key, metadata)
                return metadata

        raise ValueError(f"Failed to load task metadata for {cache_key}")

    def pop(self, key: str, default: Any) -> Any:
        cache_key = self._sanitize_key(key)
        client_key = cache_key
        client_key = client_key.removeprefix(self.prefix)

        return self.client.pop(client_key, default)

    def find(self, pattern: str):
        if not pattern.startswith(self.prefix):
            pattern = f"{self.prefix}{pattern}"

        return set(self.client.iterkeys(pattern=pattern))

    def transition_state(
        self,
        metadata: TaskMetadata,
        next_status: ExecutionStatus,
        next_step_id: str | None = None,
        **overrides,
    ) -> str:
        """Atomically rotates the cache key to prevent state race conditions and syncs the DB."""
        # 1. Purge old tracking footprint
        old_key = self._sanitize_key(metadata.generate_cache_key())
        old_client_key = old_key.removeprefix(self.prefix)
        self.client.pop(old_client_key, None)

        # 2. Apply metadata overrides
        metadata.status = next_status.value
        metadata.last_hb = time.time()
        if next_step_id:
            metadata.current_step_id = next_step_id
        for field, value in overrides.items():
            if hasattr(metadata, field):
                setattr(metadata, field, value)

        # 3. Save to active operational runtime cache
        new_key = self._sanitize_key(metadata.generate_cache_key())
        new_client_key = new_key.removeprefix(self.prefix)
        self.client.set(new_client_key, metadata)
        LOG.trace("Cache updated", old_key=old_key, new_key=new_key)

        # 4. Asynchronously or synchronously sync to the external persistent state store
        if self.state_hub:
            try:
                # Parse the metadata object cleanly to the ClickHouse schema format
                update_record = {
                    "JOB_ID": metadata.run_id,  # Maps to your logging identifiers
                    "DATASET_ID": (
                        metadata.config_file.split("/")[-2]
                        if "/" in metadata.config_file
                        else "unknown"
                    ),
                    "PARTITION_DATE": time.strftime(
                        "%Y-%m-%d", time.gmtime(metadata.last_hb)
                    ),
                    "JOB_STATUS": metadata.status,
                    "CURRENT_STEP": metadata.current_step_id,
                    "REMARKS": overrides.get("remarks"),
                    "RETRY_ATTEMPTS": getattr(metadata, "retry_count", 0),
                    "LAST_UPDATED_AT_TS_LC": current_timestamp().isoformat(sep=" "),
                }
                self.state_hub.update_task(metadata.run_id, update_record)
            except Exception as err:
                LOG.exception(
                    f"StateHub telemetry broadcast failed for {metadata.run_id}: {err}"
                )
        return new_key
