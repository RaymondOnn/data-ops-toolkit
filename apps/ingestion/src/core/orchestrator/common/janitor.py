"""Task cleanup, quarantine, and recovery operations."""

import shutil
import time
from collections.abc import Callable, Iterable
from enum import StrEnum
from pathlib import Path
from typing import Any

import pendulum
from loguru import logger

from src.core.contexts import ExecutionContext
from src.core.contexts.task import load_context
from src.core.models.task import (
    ExecutionStatus,
    Task,
    TaskRef,
    TaskSignal,
)
from src.utils.constants import (
    CONFIG_FILENAME,
    MANIFEST_FILENAME,
)
from src.utils.decorators import log_dry_run

LOG = logger


class SourceCleanupMode(StrEnum):
    """Modes for cleaning up external source files."""

    FILE = "file"
    DIRECTORY = "directory"
    DIRECTORY_IF_EMPTY = "directory_if_empty"


class Janitor:
    """Handles task cleanup, quarantine, and recovery."""

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        active_tasks_fn: Callable[[], dict],
        enqueue_fn: Callable[[TaskRef, str], Any],
    ) -> None:
        self.exec_ctx = exec_ctx
        self._get_active_tasks = active_tasks_fn
        self._enqueue = enqueue_fn
        self.exec_ctx.failed_path.mkdir(parents=True, exist_ok=True)

    # ========== Public API ==========

    def cleanup_workspace(
        self,
        expired: bool = False,
        all_data: bool = False,
        run_id: str | None = None,
        older_than_days: int | None = None,
    ) -> None:
        """Clean workspace based on criteria."""
        if run_id:
            self._purge_run(run_id)
        elif all_data:
            self._wipe_workspace()
        else:
            self._purge_tasks(expired=expired, older_than_days=older_than_days)

    def quarantine(self, folder_path: Path, category: str) -> None:
        """Move a failed task to quarantine."""
        try:
            task = Task.from_path(folder_path, self.exec_ctx)
            LOG.info(f"Quarantining {task.run_id} -> {category}")
            task.move_to(category.upper())
        except Exception:
            LOG.exception(f"Quarantine failed for {folder_path}")

    # TO-DO: Duplicate cache update?
    def recover_task(self, folder_path: Path) -> None:
        """Recover a quarantined / failed task and re-queue it."""
        try:
            task = Task.from_path(folder_path, self.exec_ctx)
            resume_step_id = task.manifest.current_step_id

            LOG.info(
                f"Recovering {task.run_id} from quarantine, resuming at {resume_step_id}"
            )

            # Reset task state
            updates = {
                "status": ExecutionStatus.PENDING,
                "current_step_id": resume_step_id,
                "error": None,
                "retry_count": 0,
            }

            # Clear bitmask for resume stage and onward
            # found = False
            # for stage in Stage:
            #     if stage.value == resume_stage:
            #         found = True
            #     if found:
            #         updates[stage.value] = None
            #         task.workspace.remove_marker(stage.value)

            task.workspace.remove_marker(resume_step_id)
            task.update_manifest(updates)
            task.move_to("active")

            # Re-queue
            updated_ref = TaskRef(
                identity=task.task_ref.identity,
                namespace=task.task_ref.namespace,
                status=ExecutionStatus.PENDING,
                step_id=resume_step_id,
            )

            self._enqueue(updated_ref, str(task.workspace.path / CONFIG_FILENAME))
            task.send_signal(TaskSignal.SYNC)

        except Exception:
            LOG.exception(f"Recovery failed for {folder_path}")

    # ========== Cleanup Operations ==========
    def cleanup_task(self, task: "Task") -> None:
        """Apply all cleanup policies to a completed task."""
        # Vault cleanup - but preserve in certain modes
        preserve = any(
            [
                self.exec_ctx.is_test,
                self.exec_ctx.is_dry_run,
                task.context.overrides.get("regression_mode", False),
            ]
        )

        if preserve:
            LOG.info(
                "Skipping vault purge (Preservation mode active)", run_id=task.run_id
            )
        else:
            LOG.debug("Purging physical data vaults", run_id=task.run_id)
            # task.purge_data_vaults()

        # External source cleanup
        # self._cleanup_external_sources(task)

        # Metadata cleanup - must be LAST (removes config/manifest needed above)
        LOG.debug("Purging workspace metadata", run_id=task.run_id)
        if task.workspace.path.exists():
            shutil.rmtree(task.workspace.path, ignore_errors=True)

        self.remove_orphaned_config(task.id, task.run_id)

    # def _cleanup_external_sources(self, task: "Task") -> None:
    #     """Clean up external source files/directories after successful ingestion."""

    #     # Skip conditions
    #     if self.exec_ctx.is_test:
    #         return

    #     extract_ctx = task.context.extract
    #     if not extract_ctx:
    #         return

    #     params = extract_ctx.params
    #     source_path = extract_ctx.resource

    #     # Check if cleanup is needed
    #     if params.get("type") != "file":
    #         return

    #     cleanup = params.get("remove_after", {})
    #     if (
    #         not cleanup.get("enabled")
    #         or not source_path
    #         or not Path(source_path).exists()
    #     ):
    #         return

    #     # Perform cleanup
    #     self._execute_source_cleanup(Path(source_path), cleanup.get("mode", "file"))

    def _execute_source_cleanup(self, path: Path, mode: str) -> None:
        """Execute the actual source cleanup based on mode."""
        mode = mode.lower()
        parent = path.parent if path.is_file() else path

        if mode == "file" and path.is_file():
            LOG.info(f"Deleting source file: {path}")
            path.unlink(missing_ok=True)

        elif mode == "directory":
            LOG.info(f"Deleting source directory: {parent}")
            shutil.rmtree(parent, ignore_errors=True)

        elif mode == "directory_if_empty":
            if path.is_file():
                path.unlink(missing_ok=True)
            if parent.is_dir() and not any(parent.iterdir()):
                LOG.info(f"Removing empty directory: {parent}")
                parent.rmdir()
        else:
            LOG.warning(f"Unknown cleanup mode '{mode}', skipping")

    def remove_orphaned_config(self, task_id: str, run_id: str) -> None:
        """Remove orphaned config file."""
        prefix = f"{task_id}:{run_id}"
        config = self.exec_ctx.active_path / f"{prefix}_{CONFIG_FILENAME}"
        if config.exists():
            config.unlink()

    # ========== Purge Operations ==========

    def _purge_run(self, run_id: str) -> None:
        """Purge a specific run by ID."""
        count = 0
        for folder in self.scan_task_folders():
            if folder.name == run_id and self._purge_folder(folder, "Targeted cleanup"):
                count += 1

        if not count:
            LOG.warning(f"Run {run_id} not found")

    def _purge_tasks(
        self, expired: bool = False, older_than_days: int | None = None
    ) -> None:
        """Unified purge for expired or old tasks."""
        now = time.time()
        cutoff = now - (older_than_days * 86400) if older_than_days else None

        count = 0
        # Only scan active_path for expiry checks, all paths for age checks
        roots = [self.exec_ctx.active_path] if expired else None

        for folder in self.scan_task_folders(roots):
            should_purge = False
            reason = None

            if expired:
                try:
                    ctx = load_context(folder)
                    if ctx.expires_at and ctx.expires_at < now:
                        ts = pendulum.from_timestamp(ctx.expires_at)
                        reason = f"Expired at {ts.to_datetime_string()}"
                        should_purge = True
                except Exception:
                    continue

            if not should_purge and cutoff and folder.stat().st_mtime < cutoff:
                reason = f"Older than {older_than_days} days"
                should_purge = True

            if should_purge and self._purge_folder(folder, reason):
                count += 1

        if expired:
            LOG.info(f"Purged {count} expired tasks")
        elif older_than_days:
            LOG.info(f"Purged {count} tasks older than {older_than_days} days")

    def _purge_folder(self, folder: Path, reason: str | None = None) -> bool:
        """Purge a task folder."""
        if self.exec_ctx.is_dry_run:
            LOG.info(f"[DRY RUN] Would purge: {folder.name} ({reason})")
            return True

        # Don't purge active tasks
        active_ids = {
            TaskRef.from_key(v).identity.run_id
            for v in self._get_active_tasks().values()
        }
        if folder.name in active_ids:
            LOG.debug(f"Skipping active task: {folder.name}")
            return False

        try:
            task = Task.from_path(folder, self.exec_ctx)
            LOG.warning(f"Purging {task.run_id}: {reason}")

            if (
                reason
                and "Expired" in reason
                and task.manifest.status != ExecutionStatus.EXPIRED
            ):
                task.update_manifest(
                    {"status": ExecutionStatus.EXPIRED, "remarks": reason}
                )

            self.cleanup_task(task)
            return True
        except Exception:
            LOG.exception("Failed to purge {folder}")
            return False

    def scan_task_folders(self, roots: list[Path] | None = None) -> Iterable[Path]:
        """Scan for task folders containing manifests."""
        if roots is None:
            roots = [self.exec_ctx.active_path, self.exec_ctx.failed_path]

        for root in roots:
            if root.exists():
                for manifest in root.rglob(MANIFEST_FILENAME):
                    yield manifest.parent

    # ========== Workspace Management ==========
    @log_dry_run
    def _wipe_workspace(self) -> None:
        """Delete entire workspace."""
        paths = list(self.exec_ctx.get_managed_directories())
        if self.exec_ctx.lock_file.exists():
            paths.append(self.exec_ctx.lock_file)

        for path in paths:
            if not path.exists():
                continue

            if path == self.exec_ctx.workspace_dir:
                LOG.error(f"Blocked from deleting workspace root: {path}")
                continue

            if self.exec_ctx.is_dry_run:
                LOG.info(f"[DRY RUN] Would delete: {path}")
                continue

            try:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
                LOG.info(f"Deleted: {path}")
            except Exception:
                LOG.exception("Failed to delete {path}")


# TODO: Can we add a 'dry_run' flag to the Janitor class to allow simulating expiry sweeps without deleting files?
# TODO: How can we implement a 'dry_run' flag in the Janitor class that logs physical purges without deleting files?
# TODO: How can we implement a 'DRY_RUN' flag in the Janitor class so I can verify these eviction rules in the logs without actually deleting data?
