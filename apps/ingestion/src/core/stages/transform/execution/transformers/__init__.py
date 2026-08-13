"""Transform strategies module with auto-discovery."""

# from .sql import SQLTransformer

# _all__ = ["SQLTransformer"]

# # Modules to skip during discovery
# _EXCLUDE: set[str] = {"__init__"}


# # Auto-discover all transform logic modules
# def _discover_modules() -> None:
#     """Auto-discover all transformer modules (works in PEX)."""
#     import pkgutil
#     import importlib

#     try:
#         # This works in PEX if __path__ is accessible
#         package_path = __path__
#         for _, modname, _ in pkgutil.iter_modules(package_path):
#             if modname not in _EXCLUDE and not modname.startswith("_"):
#                 importlib.import_module(f"{__name__}.{modname}")
#                 import logging

#                 logging.getLogger(__name__).debug(
#                     f"Discovered transformLogic: {modname}"
#                 )
#     except Exception as e:
#         import logging

#         logging.getLogger(__name__).warning(f"Auto-discovery failed: {e}")


# # Run discovery
# _discover_modules()
