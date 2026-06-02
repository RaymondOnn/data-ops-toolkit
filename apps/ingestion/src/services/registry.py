import functools
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from apps.ingestion.src.utils.constants import DISKCACHE_FILE_PATH
from libs.cache.base import KeyValueCache
from libs.cache.factory import get_cache
from libs.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerState,
    CircuitBreakerTripped,
)
from loguru import logger

LOG = logger
REGISTRY_CACHE_NAMESPACE = "svc"


class ServiceRegistry:
    """Global registry for tracking the health and status of external services.

    Decision: Shared State Mesh.
    We use a Key-Value cache (typically DiskCache) to maintain a consistent
    view of service health across all distributed Ray workers. This allows
    a failure detected by one worker to "trip" the circuit breaker for all
    others, preventing unnecessary connection attempts to unreachable hosts.
    """

    _NS: ClassVar[str] = REGISTRY_CACHE_NAMESPACE
    _cache: ClassVar[KeyValueCache | None] = None
    _signal_path: ClassVar[Path | None] = None
    _local_failures: ClassVar[dict[str, int]] = {}  # In-memory buffer for THIS Pod

    @classmethod
    def configure(
        cls, workspace_dir: Path, cache_config: dict[str, Any] | None = None
    ) -> None:
        """Initializes the registry and sets the global signal path.

        Args:
            workspace_dir: The root directory for task state and signals.
            cache_config: Optional configuration for the KeyValueCache backend.

        Decision: Centralized Initialization.
        This must be called once at process startup. By pointing to a shared
        filesystem path, we ensure that the local DiskCache instance used by
        the Orchestrator is the same one used by remote Ray workers.
        """
        if cls._cache is None:
            # Initialize signal path for automated .outage markers
            cls._signal_path = workspace_dir / "signals"

            # Use the utility factory to get a normalized cache provider
            config = cache_config or {
                "type": "diskcache",
                "filepath": DISKCACHE_FILE_PATH,
            }
            cls._cache = get_cache(Path(workspace_dir), config)

    @classmethod
    def _get_cache(cls) -> KeyValueCache:
        """Internal helper to retrieve the active cache instance.

        Returns:
            KeyValueCache: The configured cache backend.

        Raises:
            RuntimeError: If the registry has not been configured.
        """
        if cls._cache is None:
            raise RuntimeError(
                "ServiceRegistry has not been configured. "
                "Call ServiceRegistry.configure(workspace_dir) before using services."
            )
        return cls._cache

    @classmethod
    def get_status(cls, name: str) -> str:
        """Retrieves the current status of a service from the shared cache.

        Args:
            name: The unique identifier of the service.

        Returns:
            str: The service status (e.g., 'CLOSED', 'OPEN', 'HALF_OPEN').
        """
        return str(cls._get_cache().get(f"{cls._NS}:status:{name}", "CLOSED"))

    @classmethod
    def is_healthy(cls, name: str) -> bool:
        """Checks if a service is healthy (Circuit Breaker is CLOSED).

        Args:
            name: The service name.

        Returns:
            bool: True if the service status is 'CLOSED', False otherwise.
        """
        return cls.get_status(name) == "CLOSED"

    @classmethod
    def probe(cls, name: str, probe_fn: Callable[[], bool]) -> bool:
        """Attempts to verify if a service is back online.

        Args:
            name: The service name.
            probe_fn: A callable that returns True if the service is reachable.

        Returns:
            bool: True if the probe succeeded and metrics were reset.

        Decision: Active Recovery.
        Instead of waiting for a passive TTL to expire, we allow the system
        to actively verify recovery, reducing 'Time-to-Resume' for blocked jobs.
        """
        try:
            if probe_fn():
                LOG.info("Probe successful for service", service=name)
                cls.reset(name)
                return True
        except Exception as e:
            LOG.warning("Probe failed for service", service=name, error=str(e))
        return False

    @classmethod
    def update_status(cls, name: str, status: str) -> None:
        """Updates the global service status and manages physical signal files.

        Args:
            name: The service name.
            status: The new status string.

        Decision: Observability Markers.
        When a service trips to 'OPEN', we drop a physical .outage file. This
        allows DevOps engineers to see system health via a simple 'ls' command.
        """
        cls._get_cache().set(f"{cls._NS}:status:{name}", status, expire=3600)

        # Automatically manage the {service_name}.outage signal file
        if cls._signal_path:
            signal_file = cls._signal_path / f"{name.lower()}.outage"
            if status == CircuitBreakerState.OPEN:
                signal_file.touch(exist_ok=True)
            elif status == CircuitBreakerState.CLOSED:
                signal_file.unlink(missing_ok=True)

    @classmethod
    def get_last_failure_time(cls, name: str) -> float:
        """Retrieves the timestamp of the last recorded failure.

        Args:
            name: The service name.

        Returns:
            float: Unix timestamp of the last failure.
        """
        val = cls._get_cache().get(f"{cls._NS}:last_fail:{name}") or 0
        return float(val) if val is not None else 0.0

    @classmethod
    def set_last_failure_time(cls, name: str, timestamp: float) -> None:
        """Sets the global last failure timestamp for a service.

        Args:
            name: The service name.
            timestamp: The Unix timestamp to record.
        """
        cls._get_cache().set(f"{cls._NS}:last_fail:{name}", timestamp)

    @classmethod
    def get_failure_count(cls, name: str) -> int:
        """Retrieves the current consecutive failure count for a service.

        Args:
            name: The service name.

        Returns:
            int: The number of recent failures.
        """
        return int(cls._get_cache().get(f"{cls._NS}:fails:{name}", 0))

    @classmethod
    def increment_failure(cls, name: str, window_seconds: int = 5) -> int:
        """Increments and returns the failure count atomically.

        Args:
            name: The service name.
            window_seconds: Duration to treat multiple failures as one event.

        Returns:
            int: The new total failure count.

        Decision: Error Dampening (Debounce).
        To prevent a 'Thundering Herd' of workers from instantly tripping
        a breaker, we ignore increments that occur within a small temporal
        window of the last reported error.
        """
        now = time.time()
        cache = cls._get_cache()

        with cache.transact():
            last_fail_time = float(cache.get(f"{cls._NS}:last_reported:{name}", 0))
            current_fails = int(cache.get(f"{cls._NS}:fails:{name}", 0))

            # If we are within the window, ignore the increment but keep current count
            if now - last_fail_time < window_seconds:
                return current_fails

            # Outside window: increment and update timestamp
            new_total = current_fails + 1
            cache.set(f"{cls._NS}:fails:{name}", new_total, expire=3600)
            cache.set(f"{cls._NS}:last_reported:{name}", now, expire=3600)

            # Update the last failure timestamp globally
            cache.set(f"{cls._NS}:last_fail:{name}", now)

            # --- TRIP LOGIC ---
            # We keep this INSIDE the transaction to ensure that the
            # status transition is atomic with the count increment.
            if new_total >= 3 and cache.get(f"{cls._NS}:status:{name}") != "OPEN":
                LOG.warning(
                    "Circuit breaker tripping",
                    service=name,
                    failures=new_total,
                    window=window_seconds,
                )
                # We use cache.set directly to stay within the transaction
                cache.set(f"{cls._NS}:status:{name}", "OPEN", expire=3600)

            return new_total

    @classmethod
    def reset(cls, name: str) -> None:
        """Clears all failure metrics for a service.

        Args:
            name: The service name.

        Decision: Idempotency.
        We perform a bulk deletion of all metadata keys to ensure a clean
        state transition back to 'CLOSED'.
        """
        cache = cls._get_cache()
        with cache.transact():
            # Check if it was previously open to avoid log spam
            was_open = cache.get(f"{cls._NS}:status:{name}") == "OPEN"
            if was_open:
                LOG.info("Circuit breaker has been reset to CLOSED", service=name)

            # Only attempt deletion if keys exist to minimize cache I/O,
            # though DiskCache.delete is now idempotent.
            for key in [
                f"{cls._NS}:fails:{name}",
                f"{cls._NS}:last_fail:{name}",
                f"{cls._NS}:retries:{name}",
                f"{cls._NS}:last_reported:{name}",
            ]:
                cache.delete(key)
            cls.update_status(name, "CLOSED")


def protect_service(
    breaker: CircuitBreaker,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator for synchronizing local breakers with the global registry.

    Args:
        breaker: A CircuitBreaker instance.

    Returns:
        Callable: A decorator for service methods.

    Decision: Transparent Resilience.
    By wrapping service methods, we ensure that every connection attempt
    is preceded by a health check and followed by a state sync, without
    requiring the service developer to manually manage the registry.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # 1. FETCH GLOBAL STATE
            # 'self.name' refers to the DB client name (e.g., "PostgresClient")
            service_name = self.name
            status = ServiceRegistry.get_status(service_name)
            last_fail = ServiceRegistry.get_last_failure_time(service_name)
            fails = ServiceRegistry.get_failure_count(service_name)

            # 2. SYNC BREAKER INSTANCE WITH GLOBAL STATE
            # We temporarily inject the registry state into your breaker logic
            breaker.state = CircuitBreakerState(status)
            breaker.failures = fails
            breaker.last_failure_time = last_fail

            # 3. BEFORE CALL CHECK
            try:
                breaker._before_call()
            except CircuitBreakerTripped:
                # If your class updated to HALF_OPEN, sync it back to registry
                if breaker.state == CircuitBreakerState.HALF_OPEN:
                    ServiceRegistry.update_status(
                        service_name, CircuitBreakerState.HALF_OPEN
                    )
                raise

            try:
                # 4. EXECUTE
                result = func(self, *args, **kwargs)

                # SUCCESS: Reset registry
                breaker._on_success()
                ServiceRegistry.reset(service_name)
                return result

            except breaker.expected_exceptions as e:
                # FAILURE: Update registry
                breaker._on_failure(e)

                # Persist the new state to the shared cache
                ServiceRegistry.increment_failure(service_name)
                ServiceRegistry.set_last_failure_time(service_name, time.time())
                ServiceRegistry.update_status(service_name, breaker.state)

                raise e

        return wrapper

    return decorator
