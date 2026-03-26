import importlib
import pkgutil
from pathlib import Path

from .base import JobStep
from .enums import JobSteps

# Ensure all step modules are loaded so JobStep.__subclasses__() is populated.
# This enables automatic discovery of concrete step implementations.
_PKG_PATH = str(Path(__file__).parent)
for _, _MODNAME, _ in pkgutil.iter_modules([_PKG_PATH]):
    if _MODNAME not in ["__init__", "base", "enums", "utils"]:
        importlib.import_module(f".{_MODNAME}", package=__package__)


def get_step_class_by_name(name: str) -> JobStep:
    """
    Given a step name, returns the corresponding JobStep class.

    Iterates through all subclasses of JobStep and checks if the name
    attribute matches the given name.
    If no match is found, raises a ValueError.
    """
    for step_class in JobStep.__subclasses__():
        # If you have nested subclasses, you may want a recursive walk here.
        if getattr(step_class, "name", None) == name:
            # Map the string name back to the JobSteps enum member
            step_enum_member = JobSteps[name.upper()]
            return step_class(step=step_enum_member)
    raise ValueError(f"Unknown step name: {name}")
