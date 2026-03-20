from enum import StrEnum
from pathlib import Path

import msgspec


class ExecutionMode(StrEnum):
    NORMAL = "normal"
    DEBUG = "debug"
    TEST = "test"


class ExecutionContext(msgspec.Struct):
    """
    Holds global application settings that are resolved at runtime.
    Injected into major components to avoid reliance on global constants.
    """

    workspace_dir: Path
    execution_mode: ExecutionMode = ExecutionMode.NORMAL

    @property
    def active_path(self) -> Path:
        return self.workspace_dir / "active"

    @property
    def signal_path(self) -> Path:
        return self.workspace_dir / "signals"

    @property
    def data_path(self) -> Path:
        return self.workspace_dir / "data"

    @property
    def hold_path(self) -> Path:
        return self.workspace_dir / "HOLD"

    @property
    def failed_path(self) -> Path:
        return self.workspace_dir / "FAILED"

    def is_debug(self) -> bool:
        return self.execution_mode == ExecutionMode.DEBUG

    def is_test(self) -> bool:
        return self.execution_mode == ExecutionMode.TEST

    def is_normal(self) -> bool:
        return self.execution_mode == ExecutionMode.NORMAL
