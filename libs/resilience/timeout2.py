import math
import time
from dataclasses import dataclass, field
from enum import StrEnum

from loguru import logger

from libs.storage.store import Store


class TimeoutType(StrEnum):
    SCHEDULE_TO_START = "s2s"
    START_TO_CLOSE = "s2c"
    SCHEDULE_TO_CLOSE = "stc"


@dataclass
class TimeoutConfig:
    """Timeout configuration for a stage."""

    schedule_to_start: int = 60  # Max time waiting in queue
    start_to_close: int = 900  # Max time executing

    @property
    def schedule_to_close(self) -> int:
        return self.schedule_to_start + self.start_to_close


@dataclass
class RunningStats:
    """Running statistics for adaptive timeouts."""

    count: int = 0
    mean: float = 0.0
    m2: float = 0.0
    min_val: float = field(default_factory=lambda: float("inf"))
    max_val: float = field(default_factory=lambda: float("-inf"))

    def update(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

        self.min_val = min(self.min_val, value)
        self.max_val = max(self.max_val, value)

    @property
    def std(self) -> float:
        if self.count < 2:
            return 0.0
        return math.sqrt(self.m2 / (self.count - 1))

    def to_dict(self) -> dict:
        return {
            "count": self.count,
            "mean": round(self.mean, 2),
            "min": round(self.min_val, 2),
            "max": round(self.max_val, 2),
        }


class TimeoutManager:
    """
    Manages timeouts with adaptive adjustment.

    Uses running statistics (mean/median) to calculate timeouts.
    Conservative defaults for first runs, tightens as data accumulates.
    """

    # Conservative defaults for first run (5 minutes)
    DEFAULT_START_TO_CLOSE = 300
    DEFAULT_SCHEDULE_TO_START = 60

    # How many samples before using stats (vs defaults)
    MIN_SAMPLES = 5

    def __init__(self, store: Store, namespace: str = "timeout"):
        self._store = store
        self._namespace = namespace
        self._cache: dict[str, RunningStats] = {}
        self._load_all()

    def _key(self, stage: str, metric: str) -> str:
        return f"{self._namespace}:{stage}:{metric}"

    def _load_all(self) -> None:
        """Load historical statistics from store."""
        for key in self._store.keys():  # noqa
            if key.startswith(f"{self._namespace}:"):
                parts = key.split(":")
                if len(parts) == 3:
                    stage = parts[1]
                    _ = parts[2]

                    if stage not in self._cache:
                        self._cache[stage] = RunningStats()

                    data = self._store.get(key)
                    if data and isinstance(data, dict):
                        # Reconstruct RunningStats from stored data
                        stats = self._cache[stage]
                        stats.count = data.get("count", 0)
                        stats.mean = data.get("mean", 0)
                        stats.m2 = data.get("m2", 0)
                        stats.min_val = data.get("min", float("inf"))
                        stats.max_val = data.get("max", float("-inf"))

    def get_config(
        self,
        stage: str,
        multiplier: float = 3.0,
        use_median: bool = True,  # Use median (robust) or mean (sensitive)
        min_timeout: int | None = None,
        max_timeout: int | None = None,
        schedule_to_start: int | None = None,
    ) -> TimeoutConfig:
        """
        Get timeout configuration for a stage.

        Formula: timeout = statistic x multiplier

        Args:
            stage: Stage name
            multiplier: How many times the statistic to set timeout (default 3)
            use_median: Use median (robust to outliers) or mean (more sensitive)
            min_timeout: Minimum timeout value (overrides calculation)
            max_timeout: Maximum timeout value (overrides calculation)
            schedule_to_start: Timeout for queue waiting (default 60s)
        """
        schedule_to_start = schedule_to_start or self.DEFAULT_SCHEDULE_TO_START

        stats = self._cache.get(stage)

        if stats and stats.count >= self.MIN_SAMPLES:
            statistic = stats.mean
            method = "mean"

            # Calculate timeout
            start_to_close = int(statistic * multiplier)

            # Apply min/max constraints
            if min_timeout:
                start_to_close = max(start_to_close, min_timeout)
            if max_timeout:
                start_to_close = min(start_to_close, max_timeout)

            logger.debug(
                f"Timeout for {stage}: {method}={statistic:.1f}s, "
                f"count={stats.count}, timeout={start_to_close}s"
            )
        else:
            # First runs - use conservative default
            start_to_close = self.DEFAULT_START_TO_CLOSE
            sample_info = (
                f" ({stats.count if stats else 0}/{self.MIN_SAMPLES} samples)"
                if stats
                else " (no samples)"
            )
            logger.debug(f"Timeout for {stage}: default={start_to_close}s{sample_info}")

        return TimeoutConfig(
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
        )

    def record(self, stage: str, duration: float, queue_time: float = 0) -> None:
        """
        Record execution duration and update running statistics.

        Records both:
        - start_to_close (duration)
        - schedule_to_start (queue_time)
        """
        # Record start_to_close (execution time)
        stats = self._cache.get(stage)
        if not stats:
            stats = RunningStats()
            self._cache[stage] = stats

        stats.update(duration)

        # Store both metrics separately
        self._store.set(self._key(stage, "stats"), stats.to_dict())

        logger.debug(
            f"Recorded {stage}: duration={duration:.1f}s, "
            f"mean={stats.mean:.1f}s, count={stats.count} "
        )

    def check(
        self,
        start_time: float,
        config: TimeoutConfig,
        queue_start_time: float | None = None,
    ) -> tuple[bool, TimeoutType | None]:
        """Check if timeout has occurred."""
        now = time.time()

        if queue_start_time:
            queue_duration = now - queue_start_time
            if queue_duration > config.schedule_to_start:
                return True, TimeoutType.SCHEDULE_TO_START

        exec_duration = now - start_time
        if exec_duration > config.start_to_close:
            return True, TimeoutType.START_TO_CLOSE

        if queue_start_time:
            total_duration = now - queue_start_time
            if total_duration > config.schedule_to_close:
                return True, TimeoutType.SCHEDULE_TO_CLOSE

        return False, None

    def print_stats(self) -> None:
        """Print current statistics."""
        print("\n" + "=" * 60)
        print("TIMEOUT STATISTICS")
        print("=" * 60)
        for stage, stats in self._cache.items():
            print(f"\n{stage.upper()}:")
            print(f"  Samples: {stats.count}")
            print(f"  Mean:    {stats.mean:.1f}s")
            print(f"  Min:     {stats.min_val:.1f}s")
            print(f"  Max:     {stats.max_val:.1f}s")
            print(f"  Std Dev: {stats.std:.1f}s")
        print("=" * 60)
