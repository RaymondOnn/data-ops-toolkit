from enum import StrEnum
from pathlib import Path

import msgspec


class ExecutionMode(StrEnum):
    NORMAL = "normal"
    DEBUG = "debug"
    TEST = "test"


class RayMode(StrEnum):
    LOCAL = "local"
    CLUSTER = "cluster"


class ExecutionContext(msgspec.Struct):
    """
    Holds global application settings that are resolved at runtime.
    Injected into major components to avoid reliance on global constants.
    """

    workspace_dir: Path
    execution_mode: ExecutionMode = ExecutionMode.NORMAL
    ray_mode: RayMode = RayMode.CLUSTER

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
    def hold_path(self) -> Path:
        return self.workspace_dir / "HOLD"

    @property
    def failed_path(self) -> Path:
        return self.workspace_dir / "FAILED"

    @property
    def lock_file(self) -> Path:
        return self.workspace_dir / "orchestrator.lock"

    def is_debug(self) -> bool:
        return self.execution_mode == ExecutionMode.DEBUG

    def is_test(self) -> bool:
        return self.execution_mode == ExecutionMode.TEST

    def is_normal(self) -> bool:
        return self.execution_mode == ExecutionMode.NORMAL

    def get_task_identifier(self, job_id: str, dataset_id: str, run_date: str) -> str:
        """Standard format for parent folder names and cache keys."""
        return f"{job_id}:{dataset_id}:{run_date}"

    def get_run_path(
        self,
        job_id: str,
        dataset_id: str,
        run_date: str,
        run_id: str,
        category: str = "active",
    ) -> Path:
        """
        Standardizes the nested folder structure:
        {workspace}/{category}/{job_id}:{dataset}:{date}/{run_id}
        """
        identifier = self.get_task_identifier(job_id, dataset_id, run_date)
        return self.workspace_dir / category / identifier / run_id

    def get_signal_name(
        self, job_id: str, dataset_id: str, run_date: str, run_id: str, extension: str
    ) -> str:
        """Generates the standardized signal filename."""
        identifier = self.get_task_identifier(job_id, dataset_id, run_date)
        return f"{identifier}:{run_id}{extension}"

    def parse_identifier(self, full_string: str) -> tuple[str, str, str, str]:
        """Parses a full colon-delimited string back into components."""
        parts = full_string.split(":")
        if len(parts) != 4:
            raise ValueError(f"Malformed identifier string: {full_string}")
        # Returns: job_id, dataset_id, run_date, run_id
        return parts[0], parts[1], parts[2], parts[3]
