from pathlib import Path

ALWAYS_ON_MODE = False
CACHE_TASK_NAMESPACE = "task"
APP_CURRENT_ENV = "local"  # local / dev / test / production
APP_TIMEZONE_LC = "Asia/Singapore"
APP_CONFIG_ROOT = Path("./apps/ingestion/config")


# File Names
MANIFEST_FILENAME = "manifest.json"
CONFIG_FILENAME = "config.json"

# Default Governance Settings
DEFAULT_RETENTION_DAYS = 2555  # 7 Years
DEFAULT_ARCHIVE_PATH = "/mnt/archive/ingestion"
DEFAULT_PARTITION_COL = "_partition"


# App Settings
STRIP_TZ_FOR_DB = True
LOG_HIGHLIGHT_KEYS: set[str] = {"run_id", "job_id", "task_id", "request_id"}
