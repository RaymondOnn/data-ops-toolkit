from collections.abc import Iterable
from enum import StrEnum
from pathlib import Path
from typing import Any

import msgspec
from apps.ingestion.src.utils.constants import APP_TIMEZONE_LC
from loguru import logger

LOG = logger


class ExecutionMode(StrEnum):
    NORMAL = "normal"
    DEBUG = "debug"
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


class ExecutionContext(msgspec.Struct):
    """
    Holds global application settings that are resolved at runtime.
    Injected into major components to avoid reliance on global constants.
    """

    workspace_dir: Path
    timezone: str = APP_TIMEZONE_LC
    execution_mode: ExecutionMode = ExecutionMode.NORMAL
    ray_mode: RayMode = RayMode.CLUSTER
    env: str = "local"
    always_on: bool = False
    code_pex_path: Path | None = None
    deps_pex_path: Path | None = None
    cache_config: dict[str, Any] = {}
    provider_config: dict[str, str] = {}  # Config for secret provider
    disable_self_healing: bool = False
    stop_at_ts: float | None = None
    drain_timeout_secs: int = 600

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
        """Yields all core directories that must be writable."""
        yield from [
            self.workspace_dir,
            self.active_path,
            self.signal_path,
            self.state_path,
            self.data_path,
            self.failed_path,
            self.workspace_dir / "HOLD",
            self.workspace_dir / ".cache",
            self.workspace_dir / "logs",
        ]

    @property
    def is_debug(self) -> bool:
        return self.execution_mode == ExecutionMode.DEBUG

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

    def get_task_identifier(
        self, job_id: str, dataset_id: str, partition_date: str
    ) -> str:
        """Standard format for parent folder names and cache keys."""
        return f"{job_id}:{dataset_id}:{partition_date}"

    def get_run_path(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str,
        run_id: str,
        category: str = "active",
    ) -> Path:
        """
        Standardizes the nested folder structure:
        {workspace}/{category}/{job_id}:{dataset}:{date}/{run_id}
        """
        identifier = self.get_task_identifier(job_id, dataset_id, partition_date)
        return self.workspace_dir / category / identifier / run_id

    def get_signal_name(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str,
        run_id: str,
        extension: str,
    ) -> str:
        """Generates the standardized signal filename."""
        identifier = self.get_task_identifier(job_id, dataset_id, partition_date)
        return f"{identifier}:{run_id}{extension}"

    def parse_identifier(self, full_string: str) -> tuple[str, str, str, str]:
        """Parses a full colon-delimited string back into components."""
        parts = full_string.split(":")
        if len(parts) != 4:
            raise ValueError(f"Malformed identifier string: {full_string}")
        # Returns: job_id, dataset_id, partition_date, run_id
        return parts[0], parts[1], parts[2], parts[3]

    def check_serializability(self) -> bool:
        """
        Validates that the context can be serialized for Ray/Distributed execution.
        Throws an informative error if a non-picklable object has been injected.
        """
        import pickle

        try:
            # Step 1: Test individual attributes to find the culprit
            for field in self.__struct_fields__:
                val = getattr(self, field)
                try:
                    pickle.dumps(val)
                except Exception as e:
                    raise TypeError(f"Attribute '{field}' is not picklable: {e}") from e

            # Step 2: Test the whole object
            pickle.dumps(self)
            LOG.debug(
                "ExecutionContext is serializable and ready for distributed execution."
            )
            return True
        except Exception as e:
            # We raise a descriptive error to make debugging easier in Ray
            raise TypeError(f"ExecutionContext is not picklable: {e}") from e
