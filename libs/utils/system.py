from pathlib import Path
from typing import NamedTuple

import psutil


class DiskUsage(NamedTuple):
    total: int
    used: int
    free: int
    percent: float


class SystemVitals(NamedTuple):
    cpu_pct: float
    mem_total: int
    mem_available: int
    mem_pct: float


def get_disk_usage(path: Path | str) -> DiskUsage:
    """
    Retrieves disk usage statistics for the filesystem containing the given path.

    Args:
        path: The filesystem path to check.

    Returns:
        DiskUsage: A NamedTuple containing total, used, free (bytes) and percentage.
    """
    p = Path(path).expanduser().resolve()
    # Fallback to nearest existing parent if the path itself hasn't been created
    while not p.exists() and p.parent != p:
        p = p.parent
    usage = psutil.disk_usage(str(p))
    return DiskUsage(usage.total, usage.used, usage.free, usage.percent)


def get_system_vitals() -> SystemVitals:
    """Returns current CPU and Memory usage statistics."""
    mem = psutil.virtual_memory()
    return SystemVitals(
        cpu_pct=psutil.cpu_percent(interval=None),
        mem_total=mem.total,
        mem_available=mem.available,
        mem_pct=mem.percent,
    )
