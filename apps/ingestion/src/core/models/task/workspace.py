import os
import shutil
from contextlib import suppress
from pathlib import Path
from typing import Any

import msgspec
from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.task.manifest import TaskManifest
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.constants import CONFIG_FILENAME, MANIFEST_FILENAME
from loguru import logger

LOG = logger


class TaskWorkspace:
    """
    Direct local filesystem interaction for Task metadata storage.
    Optimized for EC2 local disk or Shared PVCs in K8S.
    """

    def __init__(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str,
        run_id: str,
        exec_ctx: ExecutionContext,
        category: str = "active",
    ) -> None:
        self.job_id = job_id
        self.dataset_id = dataset_id
        self.partition_date = partition_date
        self.run_id = run_id
        self.exec_ctx = exec_ctx
        self.category = category
        self.base_dir = exec_ctx.workspace_dir

    @property
    def run_path(self) -> Path:
        """Standardizes the Path for this specific run."""
        identifier = self.exec_ctx.get_task_identifier(
            self.job_id, self.dataset_id, self.partition_date
        )
        return self.base_dir / self.category / identifier / self.run_id

    @property
    def manifest_path(self) -> Path:
        return self.run_path / MANIFEST_FILENAME

    @property
    def config_path(self) -> Path:
        return self.run_path / CONFIG_FILENAME

    def exists(self) -> bool:
        return self.run_path.is_dir()

    def provision(self, source_config_path: str | Path) -> None:
        """Creates the folder and moves the frozen config into place."""
        # 1. Physically create the folder
        self.run_path.mkdir(parents=True, exist_ok=True)
        LOG.debug("Provisioned workspace folder", path=str(self.run_path))

        # 2. Relocate Config
        src = Path(source_config_path)
        if src.exists() and not self.config_path.exists():
            shutil.move(src, self.config_path)
            LOG.debug(
                "Moved config to run workspace", src=str(src), dst=str(self.config_path)
            )

    def read_manifest(self) -> TaskManifest:
        """Reads and decodes the manifest from disk."""
        if not self.manifest_path.exists():
            return TaskManifest(
                job_id=self.job_id,
                run_id=self.run_id,
                dataset_id=self.dataset_id,
                current_stage="UNKNOWN",
                bitmask=0,
                status=ExecutionStatus.UNKNOWN,
            )

        return msgspec.json.decode(self.manifest_path.read_bytes(), type=TaskManifest)

    def write_manifest(self, data: dict[str, Any]) -> None:
        """Performs a safe write of the manifest data."""
        # Note: True atomicity is filesystem-dependent.
        encoded = msgspec.json.encode(data)

        # Defensive: Ensure the run directory exists before writing.
        # This prevents FileNotFoundError during atomic swap.
        self.run_path.mkdir(parents=True, exist_ok=True)

        tmp_path = self.manifest_path.with_suffix(".tmp")

        with tmp_path.open(mode="wb") as f:
            f.write(encoded)
            with suppress(OSError):
                os.fsync(f.fileno())  # Ensure bits are physically on disk

        tmp_path.replace(self.manifest_path)

    def relocate(self, new_category: str) -> str:
        """Moves the entire workspace to a new root (e.g. active -> FAILED)."""
        old_path = self.run_path
        self.category = new_category
        new_path = self.run_path

        new_path.parent.mkdir(parents=True, exist_ok=True)

        LOG.info("Relocating workspace", src=old_path, dst=new_path)
        shutil.move(old_path, new_path)
        return str(new_path)

    def purge(self, include_vaults: bool = True) -> None:
        """Removes the metadata workspace and optionally data artifacts."""
        # 1. Physical Metadata Purge
        if self.run_path.is_dir():
            shutil.rmtree(self.run_path)

        # 2. Cleanup orphaned config in active root if applicable
        identifier = self.exec_ctx.get_task_identifier(
            self.job_id, self.dataset_id, self.partition_date
        )
        root_config = (
            self.base_dir / "active" / f"{identifier}:{self.run_id}_{CONFIG_FILENAME}"
        )
        if root_config.is_file():
            root_config.unlink()

        # 3. Vault Purge (Local high-speed disk cleanup)
        if include_vaults:
            data_root = self.exec_ctx.data_path
            if data_root.exists():
                # This still uses Path for local data vault cleaning
                # as data vaults are specifically for local SSD performance
                for stage_dir in data_root.iterdir():
                    if stage_dir.is_dir():
                        for folder in stage_dir.glob(f"{self.job_id}_*"):
                            shutil.rmtree(folder, ignore_errors=True)

    def drop_signal(self, filename: str) -> None:
        """Drops a zero-byte signal file."""
        signal_path = self.exec_ctx.signal_path / filename
        signal_path.touch(exist_ok=True)

    def touch_marker(self, name: str) -> None:
        """Creates an empty marker file (e.g., .retrying or .blocked)."""
        (self.run_path / name).touch(exist_ok=True)

    def remove_marker(self, name: str) -> None:
        """Deletes a marker file if it exists."""
        (self.run_path / name).unlink(missing_ok=True)

    def write_text(self, filename: str, content: str) -> None:
        """Writes a string to a file within the workspace."""
        (self.run_path / filename).write_text(content)

    def create_stage_marker(self, stage_name: str, target_data_path: Path) -> None:
        """
        Creates a relative symlink from the workspace to the physical data vault.
        Optimized for local filesystems/Shared PVCs.
        """
        marker_path = self.run_path / stage_name

        # Calculate relative path for portability within the mount
        # e.g., active/job/run/extract -> ../../../data/extract/folder
        # This ensures that if the PVC is mounted at a different path in another pod,
        # the link remains valid.
        rel_target = os.path.relpath(target_data_path, marker_path.parent)

        if marker_path.exists() or marker_path.is_symlink():
            marker_path.unlink()

        marker_path.symlink_to(rel_target, target_is_directory=True)
        LOG.debug("Created local stage symlink", src=str(marker_path), dst=rel_target)
