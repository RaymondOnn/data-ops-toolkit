# shared_utils/timeout.py
import time
from dataclasses import field
from datetime import datetime, timedelta
from typing import Any

import msgspec


class TimeoutBudget(msgspec.Struct):
    """Generic timeout budget tracker for multi-stage operations.

    Tracks time spent across stages and enforces overall budget limits.
    Useful for distributed workflows where each stage has individual timeouts
    but there's also a global timeout constraint.

    Example:
        budget = TimeoutBudget(
            total_budget=timedelta(minutes=30),
            stage_timeouts={"extract": timedelta(minutes=5), "load": timedelta(minutes=10)}
        )
        budget.start_stage("extract")
        # ... do work ...
        budget.end_stage("extract")

        if budget.is_exceeded():
            raise TimeoutError("Overall budget exceeded")
    """

    total_budget: timedelta
    stage_timeouts: dict[str, timedelta] = field(default_factory=dict)
    started_at: datetime | None = None
    _stage_start_times: dict[str, float] = field(default_factory=dict, init=False)
    _stage_durations: dict[str, timedelta] = field(default_factory=dict, init=False)
    _paused: bool = False
    _pause_start: float | None = None
    _elapsed_paused: timedelta = field(default_factory=lambda: timedelta(0), init=False)

    def __post_init__(self):
        if self.started_at is None:
            self.started_at = datetime.now()

    def start_stage(self, stage: str) -> None:
        """Mark the start of a stage."""
        if stage in self._stage_start_times:
            raise ValueError(f"Stage '{stage}' already started without ending")
        self._stage_start_times[stage] = time.time()

    def end_stage(self, stage: str) -> timedelta:
        """Mark the end of a stage and return its duration."""
        if stage not in self._stage_start_times:
            raise ValueError(f"Stage '{stage}' was not started")

        duration = timedelta(seconds=time.time() - self._stage_start_times[stage])
        self._stage_durations[stage] = duration

        # Check stage-specific timeout
        if stage in self.stage_timeouts and duration > self.stage_timeouts[stage]:
            raise TimeoutError(
                f"Stage '{stage}' exceeded timeout of {self.stage_timeouts[stage]}, "
                f"took {duration}"
            )

        del self._stage_start_times[stage]
        return duration

    def pause(self) -> None:
        """Pause the budget timer (e.g., for waiting on external dependencies)."""
        if self._paused:
            return
        self._paused = True
        self._pause_start = time.time()

    def resume(self) -> None:
        """Resume the budget timer."""
        if not self._paused or self._pause_start is None:
            return
        self._elapsed_paused += timedelta(seconds=time.time() - self._pause_start)
        self._paused = False
        self._pause_start = None

    @property
    def elapsed(self) -> timedelta:
        """Total elapsed time excluding paused time."""
        if self.started_at is None:
            raise ValueError("Budget has not been started")

        elapsed = timedelta(seconds=time.time() - self.started_at.timestamp())
        return elapsed - self._elapsed_paused

    @property
    def remaining(self) -> timedelta:
        """Remaining time in the budget."""
        return self.total_budget - self.elapsed

    def is_exceeded(self) -> bool:
        """Check if total budget has been exceeded."""
        return self.remaining.total_seconds() < 0

    def can_complete_stage(self, stage: str, estimated_duration: timedelta) -> bool:
        """Check if there's enough remaining budget for a stage."""
        if self.is_exceeded():
            return False
        if (
            stage in self.stage_timeouts
            # Can't exceed stage timeout
            and estimated_duration > self.stage_timeouts[stage]
        ):
            return False
        return self.remaining >= estimated_duration

    def get_stage_duration(self, stage: str) -> timedelta | None:
        """Get the duration of a completed stage."""
        return self._stage_durations.get(stage)

    def get_all_stage_durations(self) -> dict[str, timedelta]:
        """Get all completed stage durations."""
        return self._stage_durations.copy()

    def get_elapsed_per_stage(self) -> dict[str, float]:
        """Get elapsed time for currently running stages."""
        result = {}
        for stage, start_time in self._stage_start_times.items():
            result[stage] = time.time() - start_time
        return result

    def reset(self) -> None:
        """Reset the budget."""
        self._stage_start_times.clear()
        self._stage_durations.clear()
        self._elapsed_paused = timedelta(0)
        self._paused = False
        self._pause_start = None
        self.started_at = datetime.now()

    def to_dict(self) -> dict[str, Any]:
        """Serialize budget state for monitoring."""
        return {
            "total_budget": self.total_budget.total_seconds(),
            "elapsed": self.elapsed.total_seconds(),
            "remaining": self.remaining.total_seconds(),
            "is_exceeded": self.is_exceeded(),
            "stage_durations": {
                k: v.total_seconds() for k, v in self._stage_durations.items()
            },
            "active_stages": list(self._stage_start_times.keys()),
            "paused": self._paused,
            "paused_duration": self._elapsed_paused.total_seconds(),
        }

    def __str__(self) -> str:
        return (
            f"TimeoutBudget(total={self.total_budget}, "
            f"elapsed={self.elapsed}, remaining={self.remaining})"
        )


# Context manager for automatic start/end
class TimeoutStage:
    """Context manager for automatic stage timing."""

    def __init__(self, budget: TimeoutBudget, stage: str):
        self.budget = budget
        self.stage = stage

    def __enter__(self):
        self.budget.start_stage(self.stage)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.budget.end_stage(self.stage)
        return False  # Don't suppress exceptions
