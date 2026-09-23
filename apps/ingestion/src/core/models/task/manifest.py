"""Task manifest data structures for stage payloads."""

from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
from libs.utils.dict import deep_merge
from libs.utils.file import atomic_save
from loguru import logger
from msgspec import field

from src.core.models.task.status import ExecutionStatus
from src.core.stages.contracts.payload import ErrorInfo
from src.core.stages.types import StagePayload
from src.utils.constants import MANIFEST_FILENAME

if TYPE_CHECKING:
    from src.core.stages.contracts.payload import ErrorInfo

    from .workspace import TaskWorkspace

LOG = logger


# TO-DO: Split into TaskManifest, ManifestFile and ManifestView
class TaskManifest(msgspec.Struct, kw_only=True):
    """Complete task state on disk."""

    # Identity
    job_id: str
    run_id: str
    dataset_id: str

    # State
    status: ExecutionStatus
    current_step_id: str
    retry_count: int = 0
    is_empty_result_set: bool = False
    remarks: str | None = None
    rollback_stack: list[str] = field(default_factory=list)

    # List of all stage/step payloads executed so far
    payloads: list[StagePayload] = field(default_factory=list)

    # Error
    error: "ErrorInfo | None" = None


class TaskManifestFile:
    """Manages serialization, deserialization, and atomic storage operations for TaskManifest."""

    @staticmethod
    def resolve_path(
        workspace: "TaskWorkspace | None" = None,
        folder_path: Path | str | None = None,
        filepath: Path | str | None = None,
    ) -> Path:
        if filepath:
            return Path(filepath)
        if folder_path:
            return Path(folder_path) / MANIFEST_FILENAME
        if workspace:
            return workspace.manifest_file
        raise ValueError(
            "Must provide workspace, folder_path, or filepath to resolve Manifest path."
        )

    @classmethod
    def load(
        cls,
        workspace: "TaskWorkspace | None" = None,
        folder_path: Path | str | None = None,
        filepath: Path | str | None = None,
    ) -> TaskManifest:
        """
        Loads the task manifest from disk.

        If the file does not exist or is corrupted, a 'Skeleton' manifest is returned
        with status set to UNKNOWN. This allows stages to perform a first-time
        initialization safely.

        Returns:
            TaskManifest: The rehydrated or skeleton manifest object.
        """
        manifest_path = cls.resolve_path(workspace, folder_path, filepath)

        if not manifest_path.is_file():
            if workspace:
                LOG.warning(
                    f"Manifest not found at {manifest_path}. Relocating workspace to FAILED skeleton state."
                )
                return TaskManifest(
                    job_id=workspace.job_id,
                    run_id=workspace.run_id,
                    dataset_id=workspace.dataset_id,
                    current_step_id="start",
                    status=ExecutionStatus.UNKNOWN,
                )
            # workspace.relocate("FAILED")
            raise FileNotFoundError(f"Manifest not found at path: {manifest_path}")

        try:
            return msgspec.json.decode(manifest_path.read_bytes(), type=TaskManifest)
        except (msgspec.DecodeError, msgspec.ValidationError) as err:
            LOG.error(
                f"Corrupted manifest at {manifest_path}: {err}. Resetting to skeleton state in FAILED."
            )
            if workspace:
                workspace.relocate("FAILED")
                return TaskManifest(
                    job_id=workspace.job_id,
                    run_id=workspace.run_id,
                    dataset_id=workspace.dataset_id,
                    current_step_id="start",
                    status=ExecutionStatus.UNKNOWN,
                )
            raise

    # @classmethod
    # def from_path(
    #     cls, folder_path: Path | str | None = None, filepath: Path | str | None = None
    # ) -> Self:
    #     """Direct file/folder path loader."""
    #     manifest_file: Path | None = None

    #     if filepath:
    #         manifest_file = Path(filepath)
    #     elif folder_path:
    #         manifest_file = Path(folder_path) / MANIFEST_FILENAME

    #     if manifest_file is None or not manifest_file.is_file():
    #         raise FileNotFoundError(
    #             f"Failed to load manifest file at: {filepath or folder_path}"
    #         )

    #     return msgspec.json.decode(manifest_file.read_bytes(), type=cls)

    @classmethod
    def save(
        cls,
        manifest: TaskManifest,
        workspace: "TaskWorkspace | None" = None,
        folder_path: Path | str | None = None,
        filepath: Path | str | None = None,
    ) -> None:
        """
        Persists manifest data to disk atomically.

        Uses a temporary file and an atomic replace operation to ensure that
        the manifest is never in a partially-written state if a crash occurs.

        """
        manifest_path = cls.resolve_path(workspace, folder_path, filepath)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        with atomic_save(manifest_path, mode="wb") as f:
            f.write(msgspec.json.encode(manifest))

    @classmethod
    def update(
        cls,
        workspace: "TaskWorkspace",
        updates: dict[str, Any],
    ) -> TaskManifest:
        """Atomic read-merge-write update operation."""
        manifest = cls.load(workspace)
        data = msgspec.to_builtins(manifest)
        merged = deep_merge(data, updates)

        updated_manifest = msgspec.json.decode(
            msgspec.json.encode(merged), type=TaskManifest
        )
        cls.save(updated_manifest, workspace)
        return updated_manifest


class TaskManifestView:
    """Read-only query wrapper providing safe inspection methods over a TaskManifest snapshot."""

    def __init__(self, manifest: TaskManifest) -> None:
        self._manifest = manifest

    @property
    def raw(self) -> TaskManifest:
        """Expose snapshot instance."""
        return self._manifest

    @property
    def completed_step_ids(self) -> list[str]:
        """Return a deduplicated list of step_ids present in the payloads, in execution order."""
        if len(self._manifest.payloads) == 0:
            LOG.warning("There are no payloads captured in the manifest!!")
        return list(
            dict.fromkeys(p.step_id for p in self._manifest.payloads if p.step_id)
        )

    def get_upstream_step_id(self, step_id: str) -> str | None:
        """Resolves the immediately preceding completed step_id relative to self.step_id."""
        completed_ids: list[str] = self.completed_step_ids
        if not completed_ids:
            return None

        # Check if current step is in completed list (e.g. during re-runs or post-hooks)
        if step_id in completed_ids:
            curr_idx = completed_ids.index(step_id)
            return completed_ids[curr_idx - 1] if curr_idx > 0 else None

        return completed_ids[-1]

    def get_payload_for_step(self, step_id: str) -> "StagePayload | None":
        """Retrieve the latest payload associated with a specific step_id."""
        for p in reversed(self._manifest.payloads):
            if p.step_id == step_id:
                return p
        return None

    def get_partition_metadata_map(
        self, step_id: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Resolves partition_date -> dict of partition metadata attributes.

        Returns:
            {
                "2026-08-30": {
                    "source": "s3://bucket/raw/2026-08-30/",
                    "watermark_start": "2026-08-30T00:00:00Z",
                    "watermark_end": "2026-08-30T23:59:59Z",
                    "checkpoint_type": "date_range",
                }
            }
        """
        partition_map: dict[str, dict[str, Any]] = {}

        def extract_from_payload(payload):
            if hasattr(payload, "partitions"):
                for p_date, p_info in payload.partitions.items():
                    partition_map[p_date] = {
                        "source": p_info.resource,
                        # "watermark_start": p_info.checkpoint_start,
                        # "watermark_end": p_info.checkpoint_end,
                        # "checkpoint_type": p_info.checkpoint_type,
                        "file_count": p_info.file_count,
                        "row_count": p_info.row_processed,
                    }

        if step_id:
            payload = self.get_payload_for_step(step_id)
            if payload:
                extract_from_payload(payload)
            return partition_map

        # Fallback: scan all extract payloads in order
        for p in self._manifest.payloads:
            if getattr(p, "stage", None) == "extract":  # or hasattr(p, "partitions"):
                print(f"{getattr(p, 'stage', None)=}")
                extract_from_payload(p)

        return partition_map
