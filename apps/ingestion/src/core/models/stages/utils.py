from pathlib import Path
from typing import TYPE_CHECKING

from .enums import Stage

if TYPE_CHECKING:
    from .base import ExecutionStage


# Stage class registry (auto-populated)
_STAGE_CLASSES: dict[str, type["ExecutionStage"]] = {}


def register_stage(stage_name: str, stage_class: type["ExecutionStage"]) -> None:
    """Register a stage class for later lookup."""
    _STAGE_CLASSES[stage_name] = stage_class


def stage(stage_name: str):
    """Decorator to register a stage class."""

    def decorator(cls):
        register_stage(stage_name, cls)
        return cls

    return decorator


def get_stage_class(stage_name: str) -> "ExecutionStage":
    """
    Given a stage name, returns the corresponding ExecutionStage class.

    Iterates through all subclasses of ExecutionStage and checks if the name
    attribute matches the given name.
    If no match is found, raises a ValueError.
    """
    # Decision: Just-In-Time Discovery.
    # We perform a one-time discovery of all stage modules if the registry is empty.
    # This ensures that decorators are executed and classes are registered before use.
    if not _STAGE_CLASSES:
        import importlib
        import pkgutil

        # We perform a one-time discovery of all stage modules if the registry
        # is empty. Using relative imports via __package__ ensures that this
        # works correctly whether running from source or inside a PEX.
        package_path = [str(Path(__file__).parent)]
        for _, modname, _ in pkgutil.iter_modules(package_path):
            if modname not in ("base", "enums", "utils", "__init__"):
                importlib.import_module(f".{modname}", package=__package__)

    if stage_name not in _STAGE_CLASSES:
        raise ValueError(f"Unknown stage: {stage_name}: {list(_STAGE_CLASSES.keys())}")
    stage_cls = _STAGE_CLASSES[stage_name]
    stage_enum = Stage(stage_name)
    return stage_cls(stage_enum)
