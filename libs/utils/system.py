import shutil
from pathlib import Path
from typing import NamedTuple


class DiskUsage(NamedTuple):
    total: int
    used: int
    free: int
    percent: float


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
    usage = shutil.disk_usage(str(p))
    percent = (usage.used / usage.total) * 100
    return DiskUsage(usage.total, usage.used, usage.free, percent)
