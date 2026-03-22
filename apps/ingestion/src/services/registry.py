import functools
import time
from collections.abc import Callable
from typing import Any, ClassVar

import diskcache
import structlog
from src.utils.constants import DISKCACHE_FILE_PATH

from libs.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerState,
    CircuitBreakerTripped,
)

LOG = structlog.getLogger(__name__)


class ServiceRegistry:
    _cache: ClassVar[diskcache.Cache | None] = None
    _local_failures: ClassVar[dict[str, int]] = {}  # In-memory buffer for THIS Pod

    @classmethod
    def configure(cls, workspace_dir: Any) -> None:
        """
        Must be called once at startup (e.g., in Worker.__init__) before any
        service calls are made. Points the shared diskcache at the correct path.
        """
        if cls._cache is None:
            cache_path = (workspace_dir / DISKCACHE_FILE_PATH).resolve()
            cls._cache = diskcache.Cache(
                cache_path,
                timeout=10,
                settings={"sqlite_journal_mode": "wal"},
            )

    @classmethod
    def _get_cache(cls) -> diskcache.Cache:
        if cls._cache is None:
            raise RuntimeError(
                "ServiceRegistry has not been configured. "
                "Call ServiceRegistry.configure(workspace_dir) before using services."
            )
        return cls._cache

    @classmethod
    def get_status(cls, name: str) -> str:
        return str(cls._get_cache().get(f"status:{name}", "CLOSED"))

    @classmethod
    def update_status(cls, name: str, status: str) -> None:
        cls._get_cache().set(f"status:{name}", status, expire=3600)

    @classmethod
    def get_last_failure_time(cls, name: str) -> float:
        return float(cls._get_cache().get(f"last_fail:{name}", 0.0))

    @classmethod
    def set_last_failure_time(cls, name: str, timestamp: float) -> None:
        cls._get_cache().set(f"last_fail:{name}", timestamp)

    @classmethod
    def get_retry_attempts(cls, name: str) -> int:
        """Tracks consecutive recovery failures for exponential backoff."""
        return int(cls._get_cache().get(f"retries:{name}", 0))

    @classmethod
    def get_failure_count(cls, name: str) -> int:
        """Retrieves the current consecutive failure count for a service."""
        return int(cls._get_cache().get(f"fails:{name}", 0))

    @classmethod
    def increment_failure(cls, name: str, window_seconds: int = 5) -> int:
        """Increments and returns the new failure count atomically."""
        """
        Dampens failure increments. Multiple failures within 
        the window count as one to avoid swarming updates at the same time.
        """
        now = time.time()
        cache = cls._get_cache()

        with cache.transact():
            last_fail_time = float(cache.get(f"last_reported:{name}", 0))
            current_fails = int(cache.get(f"fails:{name}", 0))

            # If we are within the window, ignore the increment but keep current count
            if now - last_fail_time < window_seconds:
                return current_fails

            # Outside window: increment and update timestamp
            new_total = current_fails + 1
            cache.set(f"fails:{name}", new_total, expire=3600)
            cache.set(f"last_reported:{name}", now, expire=3600)

            # Perform Autonomous Logic: Trip the circuit if threshold reached
            if new_total >= 3:  # Example threshold
                cache.set(f"status:{name}", "OPEN", expire=300)

            return new_total

    @classmethod
    def reset(cls, name: str) -> None:
        """Clears all failure metrics upon a successful call."""
        cache = cls._get_cache()
        with cache.transact():
            cache.delete(f"fails:{name}")
            cache.delete(f"last_fail:{name}")
            cache.delete(f"retries:{name}")
            cache.set(f"status:{name}", "CLOSED")


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
