import functools
import time
from collections.abc import Callable
from pathlib import Path
from typing import ClassVar

from apps.ingestion.src.utils.constants import DISKCACHE_FILE_PATH
from libs.cache.base import KeyValueCache
from libs.cache.factory import get_cache
from libs.resilience.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
    CircuitOpen,
)
from loguru import logger

LOG = logger
REGISTRY_CACHE_NAMESPACE = "svc"


class ServiceMonitor:
    """Global registry for tracking the health and status of external services.

    Decision: Shared State Mesh.
    We use a Key-Value cache (typically DiskCache) to maintain a consistent
    view of service health across all distributed Ray workers. This allows
    a failure detected by one worker to "trip" the circuit breaker for all
    others, preventing unnecessary connection attempts to unreachable hosts.
    """

    __cache: ClassVar[KeyValueCache | None] = None
    _signal_dir: ClassVar[Path | None] = None

    @classmethod
    def setup(cls, workspace: Path, cache_config: dict | None = None) -> None:
        """Initialize the health registry."""
        if cls.__cache is None:
            cls._signal_dir = workspace / "signals"
            config = cache_config or {
                "type": "diskcache",
                "filepath": DISKCACHE_FILE_PATH,
            }
            cls.__cache = get_cache(workspace, config)

    @classmethod
    def _get_cache(cls) -> KeyValueCache:
        if cls.__cache is None:
            raise RuntimeError("ServiceMonitor not configured")
        return cls.__cache

    @classmethod
    def _key(cls, suffix: str, name: str) -> str:
        return f"svc:{suffix}:{name}"

    @classmethod
    def get_state(cls, name: str) -> str:
        """Get circuit breaker state for service."""
        return cls._get_cache().get(cls._key("state", name), BreakerState.CLOSED)

    @classmethod
    def set_state(cls, name: str, state: str) -> None:
        """Set circuit breaker state and update signal file."""
        cls._get_cache().set(cls._key("state", name), state, expire=3600)

        if cls._signal_dir:
            signal = cls._signal_dir / f"{name.lower()}.outage"
            if state == BreakerState.OPEN:
                signal.touch(exist_ok=True)
            else:
                signal.unlink(missing_ok=True)

    @classmethod
    def is_healthy(cls, name: str) -> bool:
        """Check if service is healthy."""
        return cls.get_state(name) == BreakerState.CLOSED

    @classmethod
    def get_failures(cls, name: str) -> int:
        """Get consecutive failure count."""
        return int(cls._get_cache().get(cls._key("failures", name), 0))

    @classmethod
    def get_last_failure(cls, name: str) -> float:
        """Get last failure timestamp."""
        return float(cls._get_cache().get(cls._key("last_failure", name), 0))

    @classmethod
    def record_failure(cls, name: str, debounce_seconds: int = 5) -> int:
        """Record a failure and return new count."""
        cache = cls._get_cache()
        with cache.transact():
            last_reported = float(cache.get(cls._key("last_reported", name), 0))
            failures = int(cache.get(cls._key("failures", name), 0))

            # Debounce multiple failures in quick succession
            if time.time() - last_reported < debounce_seconds:
                return failures

            new_count = failures + 1
            cache.set(cls._key("failures", name), new_count, expire=3600)
            cache.set(cls._key("last_reported", name), time.time(), expire=3600)
            cache.set(cls._key("last_failure", name), time.time(), expire=3600)

            # Trip circuit if threshold reached
            if new_count >= 3 and cls.get_state(name) != BreakerState.OPEN:
                LOG.warning(
                    f"Circuit breaker tripped for {name} (failures={new_count})"
                )
                cache.set(cls._key("state", name), BreakerState.OPEN, expire=3600)

            return new_count

    @classmethod
    def reset(cls, name: str) -> None:
        """Reset service health (close circuit)."""
        cache = cls._get_cache()
        with cache.transact():
            was_open = cache.get(cls._key("state", name)) == BreakerState.OPEN
            if was_open:
                LOG.info(f"Circuit breaker reset for {name}")

            for key in ["failures", "last_failure", "last_reported"]:
                cache.delete(cls._key(key, name))

            cls.set_state(name, BreakerState.CLOSED)

    @classmethod
    def probe(cls, name: str, probe_fn: Callable[[], bool]) -> bool:
        """Test if service is responsive and reset if successful."""
        try:
            if probe_fn():
                LOG.info(f"Probe successful for {name}")
                cls.reset(name)
                return True
        except Exception as e:
            LOG.warning(f"Probe failed for {name}: {e}")
        return False


def monitor(breaker: CircuitBreaker) -> Callable:
    """Decorator to protect service methods with circuit breaker."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            service = self.name

            # Sync local breaker with global state
            breaker.state = BreakerState(ServiceMonitor.get_state(service))
            breaker.failures = ServiceMonitor.get_failures(service)
            breaker.last_failure = ServiceMonitor.get_last_failure(service)

            try:
                breaker._check_before_call()
            except CircuitOpen:
                if breaker.state == BreakerState.HALF_OPEN:
                    ServiceMonitor.set_state(service, BreakerState.HALF_OPEN)
                raise

            try:
                result = func(self, *args, **kwargs)
                breaker._succeed()
                ServiceMonitor.reset(service)
                return result

            except breaker.tracked_exceptions as e:
                breaker._fail(e)
                ServiceMonitor.record_failure(service)
                ServiceMonitor.set_state(service, breaker.state)
                raise

        return wrapper

    return decorator
