import zoneinfo as tz
from pathlib import Path

ALWAYS_ON_MODE = False
DISKCACHE_FILE_PATH = ".cache/cache.db"
APP_CURRENT_ENV = "local"  # local / dev / test / production
APP_TIMEZONE_LC = tz.ZoneInfo("Asia/Singapore")
APP_TIMEZONE_UTC = tz.ZoneInfo("UTC")
APP_CONFIG_ROOT = Path("./apps/ingestion/config")


# File Names
MANIFEST_FILENAME = "manifest.json"
CONFIG_FILENAME = "config.json"

# Default Governance Settings
DEFAULT_RETENTION_DAYS = 2555  # 7 Years
DEFAULT_ARCHIVE_PATH = "/mnt/archive/ingestion"
DEFAULT_PARTITION_COL = "_partition"

# Disk Pressure Thresholds (Percentage)
DISK_THRESHOLD_WARNING = 85
DISK_THRESHOLD_CRITICAL = 90
DISK_THRESHOLD_HALT = 95


MISFIRE_GRACE_PERIOD_SECS = 3600