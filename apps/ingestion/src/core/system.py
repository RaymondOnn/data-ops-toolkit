# health.py
"""System health monitoring and management."""

import sys
import time
from enum import Enum
from pathlib import Path
from typing import ClassVar, Self

import msgspec
from libs.utils.system import get_disk_usage, get_system_vitals
from loguru import logger

LOG = logger

# Thresholds
DISK_THRESHOLD_WARN = 75.0
DISK_THRESHOLD_CRITICAL = 85.0
DISK_THRESHOLD_BLOCKED = 95.0
MEMORY_THRESHOLD_WARN = 80.0
MEMORY_THRESHOLD_CRITICAL = 90.0
MEMORY_THRESHOLD_BLOCKED = 95.0


class HealthStatus(Enum):
    """System health status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"  # Memory pressure
    CRITICAL = "critical"  # Disk pressure
    BLOCKED = "blocked"  # Cannot proceed

    @classmethod
    def evaluate(
        cls, value: float, warn: float, critical: float, blocked: float
    ) -> "HealthStatus":
        """Evaluate a metric against thresholds."""
        if value >= blocked:
            return HealthStatus.BLOCKED
        if value >= critical:
            return HealthStatus.CRITICAL
        if value >= warn:
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY


class SystemHealth(msgspec.Struct):
    """Current system health snapshot.

    Notes:
    - We use msgspec.Struct to ensure efficient serialization and deserialization.
    """

    disk_usage_pct: float
    memory_usage_pct: float
    disk_status: HealthStatus
    memory_status: HealthStatus
    status: HealthStatus
    last_checked: float

    @classmethod
    def from_metrics(
        cls,
        disk_usage_pct: float,
        memory_usage_pct: float,
        last_checked: float | None = None,
    ):
        # Evaluate statuses
        disk_status = HealthStatus.evaluate(
            disk_usage_pct,
            DISK_THRESHOLD_WARN,
            DISK_THRESHOLD_CRITICAL,
            DISK_THRESHOLD_BLOCKED,
        )
        memory_status = HealthStatus.evaluate(
            memory_usage_pct,
            MEMORY_THRESHOLD_WARN,
            MEMORY_THRESHOLD_CRITICAL,
            MEMORY_THRESHOLD_BLOCKED,
        )

        # Overall status is the worst of the two
        status_order = [
            HealthStatus.HEALTHY,
            HealthStatus.DEGRADED,
            HealthStatus.CRITICAL,
            HealthStatus.BLOCKED,
        ]
        status = max([disk_status, memory_status], key=status_order.index)

        # Call parent struct initializer
        return cls(
            disk_usage_pct=disk_usage_pct,
            memory_usage_pct=memory_usage_pct,
            disk_status=disk_status,
            memory_status=memory_status,
            status=status,
            last_checked=last_checked or time.time(),
        )

    def message(self) -> str:
        """Generate human-readable health message."""
        messages = {
            HealthStatus.HEALTHY: (
                f"System healthy (disk: {self.disk_usage_pct:.1f}%, "
                f"memory: {self.memory_usage_pct:.1f}%)"
            ),
            HealthStatus.DEGRADED: (
                f"System degraded (disk: {self.disk_usage_pct:.1f}%, "
                f"memory: {self.memory_usage_pct:.1f}%)"
            ),
            HealthStatus.CRITICAL: (
                f"System critical (disk: {self.disk_usage_pct:.1f}%, "
                f"memory: {self.memory_usage_pct:.1f}%)"
            ),
            HealthStatus.BLOCKED: (
                f"System blocked - disk full (disk: {self.disk_usage_pct:.1f}%, "
                f"memory: {self.memory_usage_pct:.1f}%)"
            ),
        }
        return messages.get(
            self.status,
            f"Unknown health status (disk: {self.disk_usage_pct:.1f}%, "
            f"memory: {self.memory_usage_pct:.1f}%)",
        )

    @property
    def is_degraded(self) -> bool:
        """Check if system is in degraded state."""
        return self.status in (
            HealthStatus.DEGRADED,
            HealthStatus.CRITICAL,
            HealthStatus.BLOCKED,
        )

    @property
    def can_accept_new_tasks(self) -> bool:
        """Whether new tasks can be accepted."""
        return self.status != HealthStatus.BLOCKED

    @property
    def should_throttle(self) -> bool:
        """Whether task dispatch should be throttled."""
        return self.status in (HealthStatus.DEGRADED, HealthStatus.CRITICAL)

    @property
    def is_disk_blocked(self) -> bool:
        """Check if disk is blocked."""
        return self.disk_status == HealthStatus.BLOCKED

    @property
    def is_memory_blocked(self) -> bool:
        """Check if memory is blocked."""
        return self.memory_status == HealthStatus.BLOCKED


class SystemMonitor:
    """
    System health monitoring singleton.

    Usage:
        # Initialize once with workspace directory
        monitor = SystemMonitor(workspace_dir)

        # Get current health
        health = monitor.check()

        # Check disk status
        if monitor.is_disk_blocked():
            # Handle disk pressure
            pass
    """

    _instance: ClassVar[Self | None] = None
    _last_report: ClassVar[SystemHealth | None] = None
    _workspace_dir: ClassVar[Path | None] = None
    _initialized: ClassVar[bool] = False

    def __new__(cls, workspace_dir: Path) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, workspace_dir: Path):
        # Access and modify the variables at the class level (cls)
        if not self.__class__._initialized:
            self.__class__._workspace_dir = workspace_dir
            self.__class__._initialized = True
            # Perform initial check
            self.check()

    @classmethod
    def reset(cls) -> None:
        """Reset singleton (for testing)."""
        cls._instance = None
        cls._last_report = None
        cls._workspace_dir = None
        cls._initialized = False

    def check(self) -> SystemHealth:
        """
        Check system health and cache the result.

        Returns:
            SystemHealth: Current health snapshot.
        """
        if self._workspace_dir is None:
            raise RuntimeError("SystemMonitor not initialized with workspace_dir")

        vitals = get_system_vitals()
        disk = get_disk_usage(self._workspace_dir)

        health = SystemHealth.from_metrics(
            disk_usage_pct=disk.percent,
            memory_usage_pct=vitals.mem_pct,
        )

        # Cache the result at the class level so class methods can access it
        self.__class__._last_report = health

        # Log if degraded
        if health.is_degraded:
            LOG.warning(health.message())

        return health

    @classmethod
    def get_last_report(cls) -> SystemHealth | None:
        """Get the last cached health report."""
        return cls._last_report

    @classmethod
    def is_disk_blocked(cls) -> bool:
        """
        Check if disk is currently blocked.

        Returns:
            bool: True if disk usage is at BLOCKED level.
        """
        if cls._last_report is None:
            # If no report, assume healthy (caller should check first)
            return False
        return cls._last_report.is_disk_blocked

    @classmethod
    def is_memory_blocked(cls) -> bool:
        """
        Check if memory is currently blocked.

        Returns:
            bool: True if memory usage is at BLOCKED level.
        """
        if cls._last_report is None:
            return False
        return cls._last_report.memory_status == HealthStatus.BLOCKED

    @classmethod
    def is_degraded(cls) -> bool:
        """
        Check if system is degraded.

        Returns:
            bool: True if system is in degraded state.
        """
        if cls._last_report is None:
            return False
        return cls._last_report.is_degraded

    @classmethod
    def get_disk_usage(cls) -> float | None:
        """
        Get current disk usage percentage.

        Returns:
            Optional[float]: Disk usage percentage or None if not available.
        """
        if cls._last_report is None:
            return None
        return cls._last_report.disk_usage_pct

    @classmethod
    def get_memory_usage(cls) -> float | None:
        """
        Get current memory usage percentage.

        Returns:
            Optional[float]: Memory usage percentage or None if not available.
        """
        if cls._last_report is None:
            return None
        return cls._last_report.memory_usage_pct

    @classmethod
    def validate_or_halt(cls) -> None:
        """
        Check health and halt if system is blocked.

        Raises:
            SystemExit: If system is in BLOCKED state.
        """
        if cls._last_report is None:
            # No report available - create one if we have workspace_dir
            if cls._instance is not None and cls._workspace_dir is not None:
                cls._instance.check()
            else:
                LOG.warning("Cannot validate health - SystemMonitor not initialized")
                return

        if cls._last_report and cls._last_report.status == HealthStatus.BLOCKED:
            LOG.critical(
                f"Disk full ({cls._last_report.disk_usage_pct:.1f}%). "
                "Stopping orchestrator."
            )
            sys.exit(1)

    @classmethod
    def force_refresh(cls) -> SystemHealth | None:
        """
        Force a refresh of the health check.

        Returns:
            Optional[SystemHealth]: Updated health report or None if not initialized.
        """
        if cls._instance is not None and cls._workspace_dir is not None:
            return cls._instance.check()
        return None

    @classmethod
    def get_status_message(cls) -> str:
        """
        Get a human-readable status message.

        Returns:
            str: Status message or "Unknown" if not available.
        """
        if cls._last_report is None:
            return "System status unknown"
        return cls._last_report.message()
