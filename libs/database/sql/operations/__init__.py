# Importing submodules causes all @register_compile_func decorators to run
from . import analysis, core, merge, metadata
from .base import (
    SQLOperation,
    SQLOperationType,
)

__all__ = [
    "SQLOperation",
    "SQLOperationType",
    "analysis",
    "core",
    "merge",
    "metadata",
    "register_compile_func",
]
