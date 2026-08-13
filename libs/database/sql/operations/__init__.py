# Importing submodules causes all @register_compile_func decorators to run
from . import analysis, core, merge, metadata
from .base import COMPILE_FUNCTIONS, SQLOperation, register_compile_func

__all__ = [
    "COMPILE_FUNCTIONS",
    "SQLOperation",
    "register_compile_func",
    "analysis",
    "core",
    "merge",
    "metadata",
]
