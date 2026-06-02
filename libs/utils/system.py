from pathlib import Path
from typing import NamedTuple

import psutil


class DiskUsage(NamedTuple):
    """Statistics about disk space on a specific filesystem partition.

    Attributes:
        total: Total space in bytes.
        used: Used space in bytes.
        free: Free space in bytes.
        percent: Utilization percentage (0.0 to 100.0).
    """

    total: int
    used: int
    free: int
    percent: float

    @property
    def free_gb(self) -> float:
        """Converts free bytes to Gigabytes for human-readable reporting."""
        return self.free / (1024**3)


class SystemVitals(NamedTuple):
    """Snapshot of core system resource utilization.

    Attributes:
        cpu_pct: System-wide CPU utilization percentage.
        mem_total: Total physical memory in bytes.
        mem_available: Available memory in bytes.
        mem_pct: Memory utilization percentage.
    """

    cpu_pct: float
    mem_total: int
    mem_available: int
    mem_pct: float


def get_disk_usage(path: Path | str) -> DiskUsage:
    """Retrieves disk usage statistics for the filesystem containing the given path.

    Args:
        path: The filesystem path to check.

    Returns:
        DiskUsage: A named tuple containing total, used, free bytes and
            utilization percentage.
    """
    p = Path(path).expanduser().resolve()
    # Fallback to nearest existing parent if the path itself hasn't been created
    while not p.exists() and p.parent != p:
        p = p.parent
    usage = psutil.disk_usage(str(p))
    return DiskUsage(usage.total, usage.used, usage.free, usage.percent)


def get_system_vitals() -> SystemVitals:
    """Returns current CPU and Memory usage statistics.

    Returns:
        SystemVitals: A named tuple containing current CPU and memory
            metrics across the host system.
    """
    mem = psutil.virtual_memory()
    return SystemVitals(
        cpu_pct=psutil.cpu_percent(interval=None),
        mem_total=mem.total,
        mem_available=mem.available,
        mem_pct=mem.percent,
    )
