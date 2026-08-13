"""Execution context and runtime configuration."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any

import msgspec
from loguru import logger

from src.utils.constants import APP_TIMEZONE_LC

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from src.core.models.task.enums import TaskIdentity

LOG = logger


class ExecutionMode(StrEnum):
    NORMAL = "normal"
    TEST = "test"
    DRY_RUN = "dry_run"


class RayMode(StrEnum):
    LOCAL = "local"
    CLUSTER = "cluster"


class Env(StrEnum):
    LOCAL = "local"
    DEV = "dev"
    TEST = "test"
    PROD = "prod"


class ExecutionContext(msgspec.Struct, kw_only=True):
    """Global runtime configuration injected into all components."""

    workspace_dir: Path
    timezone: str = APP_TIMEZONE_LC
    execution_mode: ExecutionMode = ExecutionMode.NORMAL
    ray_mode: RayMode = RayMode.CLUSTER
    env: str = "local"
    always_on: bool = False
    code_pex_path: Path | None = None
    deps_pex_path: Path | None = None
    cache_config: dict[str, Any] = {}
    provider_config: dict[str, str] = {}
    task_queue_config: dict[str, str] = {}
    disable_self_healing: bool = False
    stop_at_ts: float | None = None
    drain_timeout_secs: int = 600
    max_retries: int = 3

    # =========================================================================
    # Path Properties
    # =========================================================================

    @property
    def active_path(self) -> Path:
        return self.workspace_dir / "active"

    @property
    def signal_path(self) -> Path:
        return self.workspace_dir / "signals"

    @property
    def state_path(self) -> Path:
        return self.workspace_dir / "state"

    @property
    def data_path(self) -> Path:
        return self.workspace_dir / "data"

    @property
    def failed_path(self) -> Path:
        return self.workspace_dir / "FAILED"

    @property
    def lock_file(self) -> Path:
        return self.workspace_dir / "orchestrator.lock"

    def get_managed_directories(self) -> Iterable[Path]:
        """Yield all core directories that must be writable."""
        yield from [
            self.workspace_dir,
            self.active_path,
            self.signal_path,
            self.state_path,
            self.data_path,
            self.failed_path,
            self.workspace_dir / ".cache",
            self.workspace_dir / "logs",
        ]

    # =========================================================================
    # Mode Properties
    # =========================================================================

    @property
    def is_test(self) -> bool:
        return self.execution_mode == ExecutionMode.TEST

    @property
    def is_normal(self) -> bool:
        return self.execution_mode == ExecutionMode.NORMAL

    @property
    def is_dry_run(self) -> bool:
        return self.execution_mode == ExecutionMode.DRY_RUN

    @property
    def is_prod(self) -> bool:
        return self.env == Env.PROD

    # =========================================================================
    # Path Resolution
    # =========================================================================

    def get_run_path(self, identity: TaskIdentity, category: str = "active") -> Path:
        """Get task run directory path."""
        return self.workspace_dir / category / identity.key / identity.run_id

    def get_signal_name(self, identity: TaskIdentity, extension: str) -> str:
        """Generate signal filename for a task."""
        return f"{identity.key}:{identity.run_id}{extension}"

    def parse_task_id(self, full_string: str) -> TaskIdentity:
        """Parse colon-delimited string into TaskIdentity."""
        from src.core.models.task.enums import TaskIdentity

        return TaskIdentity.from_signal_stem(full_string)

    # =========================================================================
    # Serialization
    # =========================================================================

    def verify_serializable(self) -> bool:
        """Verify context can be pickled for Ray distribution."""
        import pickle

        try:
            for field in self.__struct_fields__:
                val = getattr(self, field)
                pickle.dumps(val)

            pickle.dumps(self)
            LOG.debug("ExecutionContext is serializable")
            return True
        except Exception as e:
            raise TypeError(f"ExecutionContext not picklable: {e}") from e
