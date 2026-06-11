import importlib
import pkgutil
from pathlib import Path

from loguru import logger

# Get the absolute path of the current directory
pkg_path = str(Path(__file__).parent)
discovered: list[str] = []

# We use iter_modules if we only want the top-level (database.py, api.py)
# We use walk_packages if we want to go deep into subfolders.
for info in pkgutil.iter_modules([pkg_path]):
    if info.name != "__init__" and not info.name.startswith("_"):
        try:
            # Construct the full module path relative to the app root
            # e.g., "src.core.services.database"
            importlib.import_module(f"{__name__}.{info.name}")
            discovered.append(info.name)
        except ImportError as e:
            logger.warning(f"Failed to import service {info.name}: {e}")

logger.debug(f"Discovered service modules: {discovered}")
