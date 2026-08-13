# system.py
"""System health monitoring and management."""

import time
from enum import Enum
from pathlib import Path
from typing import ClassVar, Self

import msgspec
import psutil
from loguru import logger

LOG = logger

# Thresholds
DISK_THRESHOLD_WARN = 75.0
DISK_THRESHOLD_CRITICAL = 85.0
DISK_THRESHOLD_BLOCKED = 95.0
MEMORY_THRESHOLD_WARN = 80.0
MEMORY_THRESHOLD_CRITICAL = 90.0
MEMORY_THRESHOLD_BLOCKED = 95.0


def get_disk_usage(path: Path) -> float:
    """Get disk usage percentage for a given path."""
    try:
        usage = psutil.disk_usage(str(path))
        return usage.percent
    except Exception:
        return 0.0


def get_system_vitals() -> dict:
    """Get system vitals including memory percentage."""
    try:
        mem = psutil.virtual_memory()
        return {"mem_pct": mem.percent, "cpu_pct": psutil.cpu_percent(interval=0.1)}
    except Exception:
        return {"mem_pct": 0.0, "cpu_pct": 0.0}


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
    """Current system health snapshot."""

    disk_usage_pct: float
    memory_usage_pct: float
    disk_status: HealthStatus
    memory_status: HealthStatus
    status: HealthStatus

    @classmethod
    def from_metrics(cls, disk_usage_pct: float, memory_usage_pct: float):
        """Create SystemHealth from metrics."""
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

        return cls(
            disk_usage_pct=disk_usage_pct,
            memory_usage_pct=memory_usage_pct,
            disk_status=disk_status,
            memory_status=memory_status,
            status=status,
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

    _instance: ClassVar["SystemMonitor| None"] = None
    _workspace_dir: ClassVar[Path | None] = None
    _initialized: ClassVar[bool] = False

    # Lazy-loading TTL cache configurations
    _report: ClassVar[SystemHealth | None] = None
    _report_expires_at: ClassVar[float] = 0.0
    _report_ttl_secs: ClassVar[float] = 10.0

    # Hard Resource Constraint Circuit Breaker
    _disk_blocked_until: ClassVar[float] = 0.0
    _cooldown_secs: ClassVar[float] = 300.0

    def __new__(cls, workspace_dir: Path) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._workspace_dir = Path(workspace_dir)
        return cls._instance

    def __init__(self, workspace_dir: Path):
        # Only initialize once
        if not self.__class__._initialized:
            self.__class__._workspace_dir = Path(workspace_dir)
            self.__class__._initialized = True
            # Perform initial check
            self.check()

    @classmethod
    def reset(cls) -> None:
        """Reset singleton (for testing)."""
        cls._instance = None
        cls._workspace_dir = None
        cls._report = None
        cls._initialized = False

    @property
    def report(self) -> SystemHealth:
        """Lazy-loaded report property that refreshes automatically if expired."""
        now = time.time()
        if self._report is None or now >= self._report_expires_at:
            # Cache missed or expired; trigger a fresh system diagnostic run
            self.check()
        return self._report

    def check(self) -> SystemHealth:
        """
        Check system health and cache the result.

        Returns:
            SystemHealth: Current health snapshot.
        """
        if self.__class__._workspace_dir is None:
            return SystemHealth(
                0.0,
                0.0,
                HealthStatus.HEALTHY,
                HealthStatus.HEALTHY,
                HealthStatus.HEALTHY,
            )

        try:
            now = time.time()
            vitals = get_system_vitals()
            mem_pct = vitals.get("mem_pct", 0.0)

            disk_pct = get_disk_usage(self.__class__._workspace_dir)

            disk_status = HealthStatus.evaluate(
                disk_pct,
                DISK_THRESHOLD_WARN,
                DISK_THRESHOLD_CRITICAL,
                DISK_THRESHOLD_BLOCKED,
            )
            memory_status = HealthStatus.evaluate(
                mem_pct,
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
            overall_status = max([disk_status, memory_status], key=status_order.index)

            report = SystemHealth(
                disk_usage_pct=disk_pct,
                memory_usage_pct=mem_pct,
                disk_status=disk_status,
                memory_status=memory_status,
                status=overall_status,
            )

            # Update cache timestamps
            self.__class__._report = report
            self.__class__._report_expires_at = now + self.__class__._report_ttl_secs

            # If disk pressure is critical or worse, trip the 5-minute circuit breaker window
            if disk_status in (HealthStatus.CRITICAL, HealthStatus.BLOCKED):
                self.__class__._disk_blocked_until = now + self.__class__._cooldown_secs
                LOG.error(
                    f"System disk pressure CRITICAL ({disk_pct:.1f}%). "
                    f"Tripping health circuit breaker for {self._cooldown_secs}s."
                )

            return report

        except Exception:
            LOG.exception("Failed to compile system health diagnostic check")
            return SystemHealth(
                0.0,
                0.0,
                HealthStatus.HEALTHY,
                HealthStatus.HEALTHY,
                HealthStatus.HEALTHY,
            )

    def is_disk_blocked(self) -> bool:
        """
        Check if disk is currently blocked.

        Returns:
            bool: True if disk usage is at BLOCKED level.
        """
        now = time.time()
        # Fast-path check: Is the circuit breaker currently tripped?
        if now < self.__class__._disk_blocked_until:
            return True

        # Inspect via the lazy-loaded property (will trigger check() internally if expired)
        report = self.report
        return report.disk_status in (HealthStatus.CRITICAL, HealthStatus.BLOCKED)

    def is_memory_blocked(self) -> bool:
        """
        Check if memory is currently blocked.

        Returns:
            bool: True if memory usage is at BLOCKED level.
        """
        return self.report.memory_status == HealthStatus.BLOCKED

    def is_degraded(self) -> bool:
        """
        Check if system is degraded.

        Returns:
            bool: True if system is in degraded state.
        """
        return self.report.is_degraded

    def get_disk_usage(self) -> float | None:
        """
        Get current disk usage percentage.

        Returns:
            Optional[float]: Disk usage percentage or None if not available.
        """
        if self.__class__._report is None:
            return None
        return self.__class__._report.disk_usage_pct

    def get_memory_usage(self) -> float | None:
        """
        Get current memory usage percentage.

        Returns:
            Optional[float]: Memory usage percentage or None if not available.
        """
        if self.__class__._report is None:
            return None
        return self.__class__._report.memory_usage_pct

    @classmethod
    def force_refresh(cls) -> SystemHealth | None:
        """Bypasses active cache intervals to force an immediate raw device check."""
        if cls._instance is not None:
            cls._report_expires_at = 0.0  # Invalidate current cache window instantly
            return cls._instance.report
        return None
