from pathlib import Path
import zoneinfo as tz

ALWAYS_ON_MODE = False
DISKCACHE_FILE_PATH = ".cache/cache.db"
APP_CURRENT_ENV = "local"  # local / dev / test / production
APP_TIMEZONE_LC = tz.ZoneInfo("Asia/Singapore")
APP_TIMEZONE_UTC = tz.ZoneInfo("UTC")
APP_CONFIG_ROOT = Path("./apps/ingestion/config")
