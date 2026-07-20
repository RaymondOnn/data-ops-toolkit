import functools
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from libs.resilience.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
    CircuitOpen,
)
from libs.storage.cache.base import Cache
from libs.storage.cache.factory import CacheFactory
from loguru import logger

LOG = logger
REGISTRY_CACHE_NAMESPACE = "svc"


class ServiceMonitor:
    """Global registry for tracking the health and status of external services.

    Using a global shared state for service monitoring
    is crucial for ensuring resilience in a distributed system like Ray.
    If each worker independently managed its own circuit breaker state
    without synchronization, a service failure experienced by one worker
    would not be known to others. This could lead to a cascade of
    failed requests to the same unavailable service, wasting resources
    and prolonging system instability.

    The ServiceMonitor acts as a central authority for service health.
    When one worker detects a failure and opens a circuit breaker,
    it updates the shared state. Other workers can query this state
    before attempting to connect, allowing them to fail fast and
    avoid unnecessary work.

    Furthermore, the ServiceMonitor supports dynamic recovery by
    allowing the circuit breaker state to be reset. This enables
    the system to automatically adapt to transient network issues or
    service restarts without manual intervention.

    Notes:
    - We use a Key-Value cache (typically DiskCache) to maintain a consistent
    view of service health across all distributed Ray workers. This allows
    a failure detected by one worker to "trip" the circuit breaker for all
    others, preventing unnecessary connection attempts to unreachable hosts.
    - Signal directory is used to propagate service health status to other
    processes.
    """

    __cache: ClassVar[Cache | None] = None
    _signal_dir: ClassVar[Path | None] = None

    @classmethod
    def setup(cls, signal_dir: str | Path, cache_config: dict[str, Any]) -> None:
        """Initialize the health registry."""
        if cls.__cache is None:
            cls._signal_dir = Path(signal_dir)
            cls.__cache = CacheFactory.create(
                **{
                    "key": cache_config["key"],
                    "directory": cache_config["directory"],
                    "namespace": REGISTRY_CACHE_NAMESPACE,
                    "size_limit": cache_config.get("size_limit", 2**30),
                    "timeout": cache_config.get("timeout", 5),
                },
            )

    @classmethod
    def _get_cache(cls) -> Cache:
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
        cls._get_cache().set(cls._key("state", name), state)

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

        last_reported_key = cls._key("last_reported", name)
        failures_key = cls._key("failures", name)
        last_failure_key = cls._key("last_failure", name)
        state_key = cls._key("state", name)

        last_reported = float(cache.get(last_reported_key, 0))
        failures = int(cache.get(failures_key, 0))

        # Debounce multiple failures in quick succession
        if time.time() - last_reported < debounce_seconds:
            return failures

        new_count = failures + 1
        cache.set(failures_key, new_count)
        cache.set(last_reported_key, time.time())
        cache.set(last_failure_key, time.time())

        # Trip circuit if threshold reached
        if new_count >= 3 and cls.get_state(name) != BreakerState.OPEN:
            LOG.warning(f"Circuit breaker tripped for {name} (failures={new_count})")
            cache.set(state_key, BreakerState.OPEN, ttl=3600)

        return new_count

    @classmethod
    def reset(cls, name: str) -> None:
        """Reset service health (close circuit)."""
        cache = cls._get_cache()

        was_open = cache.get(cls._key("state", name)) == BreakerState.OPEN
        if was_open:
            LOG.info(f"Circuit breaker reset for {name}")

        for suffix in ["failures", "last_failure", "last_reported"]:
            cache.delete(cls._key(suffix, name))

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

            breaker.state = BreakerState(ServiceMonitor.get_state(service))
            breaker.failures = ServiceMonitor.get_failures(service)
            breaker.last_failure = ServiceMonitor.get_last_failure(service)

            try:
                breaker.check_before_call()
            except CircuitOpen:
                if breaker.state == BreakerState.HALF_OPEN:
                    ServiceMonitor.set_state(service, BreakerState.HALF_OPEN)
                raise

            try:
                result = func(self, *args, **kwargs)
                breaker.succeed()
                ServiceMonitor.reset(service)
                return result

            except breaker.tracked_exceptions:
                breaker.fail()
                ServiceMonitor.record_failure(service)
                ServiceMonitor.set_state(service, breaker.state)
                raise

        return wrapper

    return decorator
