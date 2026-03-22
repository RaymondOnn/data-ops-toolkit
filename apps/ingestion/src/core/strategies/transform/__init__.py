import importlib
import pkgutil
from pathlib import Path

import structlog

from .base import TransformContext, Transformer
from .factory import TransformFactory

LOG = structlog.getLogger(__name__)


# 1. Discover CORE transformers (bitmask, default, etc.)
# These are located in src.core.strategies.transform.transform
import src.core.strategies.transform.transform as _

# 2. Dynamically Discover CUSTOM/SHARED transformers
# We walk the 'custom' subdirectory recursively
pkg_path = Path(__file__).parent / "custom"
prefix = f"{__name__}.custom."

if pkg_path.exists():
    for _, modname, ispkg in pkgutil.walk_packages([str(pkg_path)], prefix):
        try:
            importlib.import_module(modname)
        except Exception as e:
            # In Startup Discovery, we want to know IF a module failed to load
            LOG.error(f"Failed to load transformer module: {modname}", error=str(e))
            raise e

__all__ = [
    "TransformContext",
    "TransformFactory",
    "Transformer",
]


# # Define the root of your custom transforms
# custom_root = Path(__file__).parent / "custom"

# # We use pkgutil to walk the package tree
# # This works in EC2 and inside K8S Docker images
# for loader, module_name, is_pkg in pkgutil.walk_packages(
#     path=[str(custom_root)],
#     prefix="src.core.strategies.transform.custom."
# ):
#     importlib.import_module(module_name)
