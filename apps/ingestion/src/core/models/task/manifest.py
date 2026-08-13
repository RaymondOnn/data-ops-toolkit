"""Task manifest data structures for stage payloads."""

from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
from loguru import logger
from msgspec import field

from src.core.models.task.status import ExecutionStatus
from src.core.stages.contracts.payload import ErrorInfo
from src.core.stages.types import StagePayload
from src.utils.constants import MANIFEST_FILENAME

if TYPE_CHECKING:
    from src.core.stages.contracts.payload import ErrorInfo

LOG = logger


class TaskManifest(msgspec.Struct, kw_only=True):
    """Complete task state on disk."""

    # Identity
    job_id: str
    run_id: str
    dataset_id: str

    # State
    status: ExecutionStatus
    current_step_id: str
    bitmask: int
    retry_count: int = 0
    remarks: str | None = None

    # List of all stage/step payloads executed so far
    payloads: list[StagePayload] = field(default_factory=list)

    # Error
    error: "ErrorInfo | None" = None

    @property
    def completed_step_ids(self) -> list[str]:
        """Return a deduplicated list of step_ids present in the payloads, in execution order."""
        if len(self.payloads) == 0:
            LOG.warning("There are no payloads captured in the manifest!!")
        return list(dict.fromkeys(p.step_id for p in self.payloads if p.step_id))

    @classmethod
    def from_path(
        cls, folder_path: Path | str | None = None, filepath: Path | str | None = None
    ):
        manifest_file: Path | None = None

        if filepath:
            manifest_file = Path(filepath)
        elif folder_path:
            manifest_file = Path(folder_path) / MANIFEST_FILENAME

        if manifest_file is None or not manifest_file.is_file():
            raise FileNotFoundError(
                f"Failed to load manifest file at: {filepath or folder_path}"
            )

        return msgspec.json.decode(manifest_file.read_bytes(), type=cls)

    # def get_payloads_for_stage(self, stage: str) -> list[StagePayload]:
    #     """Retrieve all payload instances for a given stage type (e.g., 'extract')."""
    #     return [p for p in self.payloads if p.stage == stage]

    def get_payload_for_step(self, step_id: str) -> "StagePayload | None":
        """Retrieve the latest payload associated with a specific step_id."""
        for p in reversed(self.payloads):
            if p.step_id == step_id:
                return p
        return None
