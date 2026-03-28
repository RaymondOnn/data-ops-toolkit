import importlib
import pkgutil
from pathlib import Path

from .base import ExecutionStage
from .enums import StageName

# Ensure all stage modules are loaded so StageName.__subclasses__() is populated.
# This enables automatic discovery of concrete stage implementations.
_PKG_PATH = str(Path(__file__).parent)
for _, _MODNAME, _ in pkgutil.iter_modules([_PKG_PATH]):
    if _MODNAME not in ["__init__", "base", "enums", "utils"]:
        importlib.import_module(f".{_MODNAME}", package=__package__)


def get_stage_class_by_name(name: str) -> ExecutionStage:
    """
    Given a stage name, returns the corresponding ExecutionStage class.

    Iterates through all subclasses of ExecutionStage and checks if the name
    attribute matches the given name.
    If no match is found, raises a ValueError.
    """
    for stage_class in ExecutionStage.__subclasses__():
        # If you have nested subclasses, you may want a recursive walk here.
        if getattr(stage_class, "name", None) == name:
            # Map the string name back to the ExecutionStage enum member
            stage_enum_member = StageName[name.upper()]
            return stage_class(stage=stage_enum_member)
    raise ValueError(f"Unknown stage name: {name}")
