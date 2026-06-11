"""Task workspace management for local filesystem operations."""

import os
import shutil
from contextlib import suppress
from pathlib import Path
from typing import Any

import msgspec
from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.core.models.task.manifest import TaskManifest
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.constants import CONFIG_FILENAME, MANIFEST_FILENAME
from loguru import logger

LOG = logger


def get_commit_hash() -> str:
    """
    Retrieves the short git commit hash for the current HEAD.

    Returns:
        str: The 7-character commit hash, or 'unknown' if git is unavailable.
    """
    import subprocess

    try:
        # Returns the short hash (e.g., a1b2c3d)
        return (
            subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
            .decode("ascii")
            .strip()
        )
    except Exception:
        return "unknown"


class TaskWorkspace:
    """
    Local filesystem manager for task metadata and artifacts.

    This class handles the physical layout of a task's run directory,
    managing the manifest, configuration, and data vault symlinks. It ensures
    that file operations are atomic and paths are deterministic.

    Attributes:
        job_id (str): Unique identifier for the job.
        dataset_id (str): Unique identifier for the dataset.
        partition_date (str): The logical data partition date.
        run_id (str): Unique identifier for this specific execution.
        exec_ctx (ExecutionContext): Global execution context for path resolution.
        category (str): The top-level folder category (e.g., 'active', 'FAILED').
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
        """
        Initializes the TaskWorkspace.

        Args:
            job_id: ID of the job.
            dataset_id: ID of the dataset.
            partition_date: Partition date (YYYY-MM-DD).
            run_id: Unique run ID.
            exec_ctx: Global execution context.
            category: Target root folder (defaults to 'active').
        """
        self.job_id = job_id
        self.dataset_id = dataset_id
        self.partition_date = partition_date
        self.run_id = run_id
        self.exec_ctx = exec_ctx
        self._category = category

    @property
    def category(self) -> str:
        """Returns the current workspace category (e.g., ACTIVE, FAILED)."""
        return self._category

    @category.setter
    def category(self, value: str) -> None:
        """Sets the workspace category, forcing uppercase for consistency."""
        self._category = value.upper()

    @property
    def _identity(self):
        """
        Returns a TaskIdentity object for the current workspace.

        Lazy-loaded to avoid circular imports.
        """
        from .enums import TaskIdentity

        return TaskIdentity(
            job_id=self.job_id,
            dataset_id=self.dataset_id,
            partition_date=self.partition_date,
            run_id=self.run_id,
        )

    @property
    def path(self) -> Path:
        """
        Constructs the full absolute path to the workspace directory.

        Returns:
            Path: The resolved directory path.
        """
        return self.exec_ctx.get_run_path(self._identity, category=self.category)

    @property
    def manifest_file(self) -> Path:
        """Returns the path to the manifest.json file."""
        return self.path / MANIFEST_FILENAME

    @property
    def config_file(self) -> Path:
        """Returns the path to the config.json file."""
        return self.path / CONFIG_FILENAME

    def get_data_path(self, stage: str) -> Path:
        """
        Constructs the data vault path for a specific pipeline stage.

        Args:
            stage: The name of the stage (e.g., 'extract').

        Returns:
            Path: The path where stage-specific artifacts are stored.
        """
        return (
            self.exec_ctx.data_path
            / self.job_id
            / self.dataset_id
            / self.partition_date
            / self.run_id
            / stage
        )

    def exists(self) -> bool:
        """
        Verifies if the workspace directory exists on disk.

        Returns:
            bool: True if it exists and is a directory.
        """
        return self.path.is_dir()

    def create(self, config_source: str | Path) -> None:
        """
        Provisions the workspace directory and moves the config file into place.

        Args:
            config_source: The source path of the configuration file.
        """
        self.path.mkdir(parents=True, exist_ok=True)
        LOG.debug(f"Created workspace: {self.path}")

        src = Path(config_source)
        if src.exists() and not self.config_file.exists():
            shutil.move(str(src), str(self.config_file))
            LOG.debug(f"Moved config to: {self.config_file}")

    def load_manifest(self) -> TaskManifest:
        """
        Loads the task manifest from disk.

        If the file does not exist, a 'Skeleton' manifest is returned with
        status set to UNKNOWN. This allows stages to perform a first-time
        initialization safely.

        Returns:
            TaskManifest: The rehydrated or skeleton manifest object.
        """
        if not self.manifest_file.exists():
            LOG.debug(f"Manifest not found. Initializing skeleton for {self.run_id}")
            return TaskManifest(
                job_id=self.job_id,
                run_id=self.run_id,
                dataset_id=self.dataset_id,
                current_stage=Stage.START.value,
                bitmask=0,
                status=ExecutionStatus.UNKNOWN,
            )

        return msgspec.json.decode(self.manifest_file.read_bytes(), type=TaskManifest)

    def save_manifest(self, data: dict[str, Any]) -> None:
        """
        Persists manifest data to disk atomically.

        Uses a temporary file and an atomic replace operation to ensure that
        the manifest is never in a partially-written state if a crash occurs.

        Args:
            data: The manifest data as a dictionary.
        """

        self.path.mkdir(parents=True, exist_ok=True)

        tmp = self.manifest_file.with_suffix(".tmp")

        # Write to temporary file
        with tmp.open("wb") as f:
            f.write(msgspec.json.encode(data))
            # fsync works on the open file handle, not the Path
            with suppress(OSError):
                os.fsync(f.fileno())

        # Atomic replace
        tmp.replace(self.manifest_file)
        # LOG.debug(f"Manifest file successfully updated: {self.manifest_file.resolve()}")

    def relocate(self, new_category: str) -> str:
        """Move workspace to a new category folder."""
        old_path = self.path
        self.category = new_category
        new_path = self.path

        new_path.parent.mkdir(parents=True, exist_ok=True)

        LOG.info(f"Relocating workspace: {old_path} -> {new_path}")
        shutil.move(str(old_path), str(new_path))
        return str(new_path)

    def delete(self, include_data: bool = True) -> None:
        """Delete workspace and optionally data artifacts."""
        # Delete metadata folder
        if self.path.is_dir():
            shutil.rmtree(self.path)

        # Delete orphaned config seed
        root_config = (
            self.exec_ctx.active_path
            / f"{self._identity.identifier}:{self.run_id}_{CONFIG_FILENAME}"
        )
        root_config.unlink(missing_ok=True)

        # Delete data vault
        if include_data:
            data_root = self.get_data_path("").parent
            if data_root.is_dir():
                LOG.debug(f"Purging data vault: {data_root}")
                shutil.rmtree(data_root)

                # Clean up empty parent directories
                for parent in data_root.parents:
                    if parent == self.exec_ctx.data_path:
                        break
                    try:
                        if not any(parent.iterdir()):
                            parent.rmdir()
                            LOG.trace(f"Removed empty parent: {parent}")
                        else:
                            break
                    except OSError:
                        break

    # =========================================================================
    # Signal Helpers
    # =========================================================================

    def send_signal(self, filename: str) -> None:
        """Create a zero-byte signal file."""
        (self.exec_ctx.signal_path / filename).touch(exist_ok=True)

    def create_marker(self, name: str) -> None:
        """Create a marker file in workspace."""
        (self.path / name).touch(exist_ok=True)

    def remove_marker(self, name: str) -> None:
        """Remove a marker file."""
        marker = self.path / name
        if marker.exists():
            if marker.is_dir() and not marker.is_symlink():
                shutil.rmtree(marker)
            else:
                marker.unlink()

    def write_text(self, filename: str, content: str) -> None:
        """Write text to a file in workspace."""
        (self.path / filename).write_text(content)

    def create_symlink(self, stage: str, data_path: Path) -> None:
        """Create symlink from workspace to stage data vault."""
        link = self.path / stage

        if link.exists() or link.is_symlink():
            link.unlink()

        rel_target = os.path.relpath(data_path, link.parent)
        link.symlink_to(rel_target, target_is_directory=True)
        LOG.debug(f"Created stage link: {link} -> {rel_target}")

    def reset_data_dir(self, stage: str) -> Path:
        """Clear and prepare data vault for a stage."""
        path = self.get_data_path(stage)
        if path.is_dir():
            LOG.debug(f"Cleaning stage data: {stage}")
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
        return path
