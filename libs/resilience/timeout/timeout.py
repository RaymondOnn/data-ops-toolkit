import logging
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Any

LOG = logging.getLogger(__name__)


class TimeoutType(StrEnum):
    SCHEDULE_TO_START = "s2s"
    START_TO_CLOSE = "s2c"
    SCHEDULE_TO_CLOSE = "stc"


@dataclass
class TimeoutConfig:
    """Configuration for a single timeout type"""

    value: timedelta
    enabled: bool = True
    description: str = ""


class TimeoutManager:
    """Simple manager for handling timeouts in the ingestion app"""

    def __init__(self):
        # Default timeout values (configurable)
        self._defaults = {
            TimeoutType.SCHEDULE_TO_START: timedelta(minutes=5),
            TimeoutType.START_TO_CLOSE: timedelta(minutes=10),
            TimeoutType.SCHEDULE_TO_CLOSE: timedelta(minutes=15),
        }

        # Override configurations
        self._configs: dict[TimeoutType, TimeoutConfig] = {}

        # Initialize with defaults
        for timeout_type in TimeoutType:
            self._configs[timeout_type] = TimeoutConfig(
                value=self._defaults[timeout_type],
                enabled=True,
                description=f"Default {timeout_type.value} timeout",
            )

    def set_timeout(
        self,
        timeout_type: TimeoutType,
        value: timedelta,
        enabled: bool = True,
        description: str | None = None,
    ) -> None:
        """
        Set timeout configuration for a specific timeout type.

        Args:
            timeout_type: Type of timeout to configure
            value: Timeout duration
            enabled: Whether this timeout is active
            description: Optional description
        """
        if value.total_seconds() <= 0:
            raise ValueError(f"Timeout value must be positive for {timeout_type.value}")

        self._configs[timeout_type] = TimeoutConfig(
            value=value,
            enabled=enabled,
            description=description or f"Custom {timeout_type.value} timeout",
        )

    def get_timeout(self, timeout_type: TimeoutType) -> timedelta | None:
        """
        Get the timeout duration for a specific type.

        Returns:
            timedelta if timeout is enabled and configured, else None
        """
        config = self._configs.get(timeout_type)
        if config and config.enabled:
            return config.value
        return None

    def get_all_timeouts(self) -> dict[str, Any]:
        """Get all current timeout configurations"""
        return {
            t.value: {
                "value": config.value.total_seconds(),
                "enabled": config.enabled,
                "description": config.description,
            }
            for t, config in self._configs.items()
        }

    def enable_timeout(self, timeout_type: TimeoutType) -> None:
        """Enable a timeout type"""
        if timeout_type in self._configs:
            self._configs[timeout_type].enabled = True

    def disable_timeout(self, timeout_type: TimeoutType) -> None:
        """Disable a timeout type"""
        if timeout_type in self._configs:
            self._configs[timeout_type].enabled = False

    def reset_to_defaults(self) -> None:
        """Reset all timeouts to default values"""
        for timeout_type in TimeoutType:
            self._configs[timeout_type] = TimeoutConfig(
                value=self._defaults[timeout_type],
                enabled=True,
                description=f"Default {timeout_type.value} timeout",
            )

    def is_timeout_exceeded(
        self, timeout_type: TimeoutType, elapsed: timedelta
    ) -> bool:
        """
        Check if the elapsed time exceeds the configured timeout.

        Args:
            timeout_type: Type of timeout to check
            elapsed: Elapsed time since operation started

        Returns:
            True if timeout is exceeded, False otherwise
        """
        timeout = self.get_timeout(timeout_type)
        if timeout is None:
            return False
        return elapsed > timeout

    def get_remaining_time(
        self, timeout_type: TimeoutType, elapsed: timedelta
    ) -> timedelta | None:
        """
        Get the remaining time before timeout.

        Returns:
            Remaining time as timedelta, or None if no timeout configured
        """
        timeout = self.get_timeout(timeout_type)
        if timeout is None:
            return None
        remaining = timeout - elapsed
        return remaining if remaining.total_seconds() > 0 else timedelta(0)


# Example usage
if __name__ == "__main__":
    manager = TimeoutManager()

    # Configure custom timeouts
    manager.set_timeout(TimeoutType.SCHEDULE_TO_START, timedelta(minutes=3))

    manager.set_timeout(
        TimeoutType.START_TO_CLOSE,
        timedelta(minutes=8),
        description="Data processing timeout",
    )

    # Check timeout status
    elapsed = timedelta(minutes=4)

    for timeout_type in TimeoutType:
        timeout = manager.get_timeout(timeout_type)
        exceeded = manager.is_timeout_exceeded(timeout_type, elapsed)
        remaining = manager.get_remaining_time(timeout_type, elapsed)

        LOG.info(f"{timeout_type.value}: {timeout} (exceeded: {exceeded})")
        if remaining:
            LOG.info(f"  Remaining: {remaining}")
