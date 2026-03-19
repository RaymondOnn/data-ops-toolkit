import pkgutil
import importlib
from pathlib import Path

# Get the absolute path of the current directory
pkg_path = str(Path(__file__).parent)

# We use iter_modules if we only want the top-level (database.py, api.py)
# We use walk_packages if we want to go deep into subfolders.
for _, modname, ispkg in pkgutil.iter_modules([pkg_path]):
    if modname != "__init__":
        # Construct the full module path relative to the app root
        # e.g., "src.core.services.database"
        importlib.import_module(f"{__name__}.{modname}")
