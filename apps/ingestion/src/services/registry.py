import functools
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from apps.ingestion.src.utils.constants import DISKCACHE_FILE_PATH
from libs.cache.base import KeyValueCache
from libs.cache.utils import get_cache
from libs.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerState,
    CircuitBreakerTripped,
)
from loguru import logger

LOG = logger
REGISTRY_CACHE_NAMESPACE = 'svc'

class ServiceRegistry:
    _NS: ClassVar[str] = REGISTRY_CACHE_NAMESPACE
    _cache: ClassVar[KeyValueCache | None] = None
    _signal_path: ClassVar[Path | None] = None
    _local_failures: ClassVar[dict[str, int]] = {}  # In-memory buffer for THIS Pod

    @classmethod
    def configure(
        cls, workspace_dir: Any, cache_config: dict[str, Any] | None = None
    ) -> None:
        """
        Must be called once at startup (e.g., in Worker.__init__) before any
        service calls are made. Points the shared diskcache at the correct path.
        """
        if cls._cache is None:
            # Initialize signal path for automated .source_down markers
            cls._signal_path = Path(workspace_dir) / "signals"

            # Use the utility factory to get a normalized cache provider
            config = cache_config or {
                "type": "diskcache",
                "filepath": DISKCACHE_FILE_PATH,
            }
            cls._cache = get_cache(Path(workspace_dir), config)

    @classmethod
    def _get_cache(cls) -> KeyValueCache:
        if cls._cache is None:
            raise RuntimeError(
                "ServiceRegistry has not been configured. "
                "Call ServiceRegistry.configure(workspace_dir) before using services."
            )
        return cls._cache

    @classmethod
    def get_status(cls, name: str) -> str:
        return str(cls._get_cache().get(f"{cls._NS}:status:{name}", "CLOSED"))

    @classmethod
    def is_healthy(cls, name: str) -> bool:
        """Checks if the service is CLOSED and no .down signal exists."""
        return cls.get_status(name) == "CLOSED"

    @classmethod
    def probe(cls, name: str, probe_fn: Callable[[], bool]) -> bool:
        """
        Attempts to verify if a service is back online.
        If successful, resets the circuit breaker.

        Use this in the Orchestrator or Janitor to verify recovery
        before re-queuing blocked tasks.
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
        cls._get_cache().set(f"{cls._NS}:status:{name}", status, expire=3600)

        # Automatically manage the {service_name}.source_down signal file
        if cls._signal_path:
            signal_file = cls._signal_path / f"{name.lower()}.source_down"
            if status == "OPEN":
                signal_file.touch(exist_ok=True)
            elif status == "CLOSED":
                signal_file.unlink(missing_ok=True)

    @classmethod
    def get_last_failure_time(cls, name: str) -> float:
        val = cls._get_cache().get(f"{cls._NS}:last_fail:{name}") or 0
        return float(val) if val is not None else 0.0

    @classmethod
    def set_last_failure_time(cls, name: str, timestamp: float) -> None:
        cls._get_cache().set(f"{cls._NS}:last_fail:{name}", timestamp)

    # @classmethod
    # def get_retry_attempts(cls, name: str) -> int:
    #     """Tracks consecutive recovery failures for exponential backoff."""
    #     return int(cls._get_cache().get(f"retries:{name}", 0))

    @classmethod
    def get_failure_count(cls, name: str) -> int:
        """Retrieves the current consecutive failure count for a service."""
        return int(cls._get_cache().get(f"{cls._NS}:fails:{name}", 0))

    @classmethod
    def increment_failure(cls, name: str, window_seconds: int = 5) -> int:
        """
        Increments and returns the failure count atomically.
        Dampens increments: multiple failures within the window count as one.
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
        """Clears all failure metrics upon a successful call."""
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
    """
    Enhanced decorator that uses the CircuitBreaker logic
    backed by the global ServiceRegistry.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        """
        Decorator that wraps a function with CircuitBreaker logic
        backed by the global ServiceRegistry.

        It fetches the global state from the ServiceRegistry,
        syncs the breaker instance with the global state,
        checks for a tripped breaker before calling the function,
        executes the function, and then updates the global state
        based on the breaker's state.

        If the breaker is tripped, it will raise a CircuitBreakerTripped
        exception. If the function execution raises an exception,
        it will update the global state accordingly.

        :param func: The function to be wrapped
        :return: The wrapped function
        """

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
