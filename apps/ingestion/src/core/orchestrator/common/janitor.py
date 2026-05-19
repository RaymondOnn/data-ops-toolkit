import shutil
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import pendulum
from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.contexts.task import load_task_context
from apps.ingestion.src.core.models.states import ExpiredState
from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskRef
from apps.ingestion.src.core.strategies.cleanup.cleanup import CleanupCoordinator
from apps.ingestion.src.utils.constants import (
    CONFIG_FILENAME,
    MANIFEST_FILENAME,
)
from loguru import logger

from .state import StateStore

LOG = logger


class Janitor:
    def __init__(
        self,
        exec_ctx: ExecutionContext,
        state_store: StateStore,
        active_tasks_fn: Callable[[], dict],
    ) -> None:
        self.state_store = state_store
        self.active_tasks_fn = active_tasks_fn
        self.exec_ctx = exec_ctx

        # Decentralized: Janitor owns the quarantine and logging zones
        self.exec_ctx.failed_path.mkdir(parents=True, exist_ok=True)

    def clean_workspace(
        self,
        expired: bool,
        all_data: bool,
        dry_run: bool,
        run_id: str | None = None,
        older_than_days: int | None = None,
    ) -> None:
        """Orchestrates workspace cleanup based on CLI flags."""
        if run_id:
            LOG.info(f"Targeted cleanup requested for Run ID: {run_id}")
            self._clean_run_id(run_id, dry_run)
            return

        if all_data:
            LOG.warning("⚠️ Full workspace wipe requested.")
            self._clean_all(dry_run)
            return

        if expired:
            LOG.info("Running expiration sweep...")
            self._clean_expired(dry_run)

        if older_than_days:
            LOG.info(f"Running age-based sweep (Older than {older_than_days} days)...")
            self._clean_older_than(older_than_days, dry_run)

    def _get_search_roots(self) -> list[Path]:
        """Returns the primary roots where task folders are stored."""
        return [
            self.exec_ctx.active_path,
            self.exec_ctx.failed_path,
            self.exec_ctx.workspace_dir / "HOLD",
        ]

    def _discover_task_folders(self, roots: list[Path]) -> Iterable[Path]:
        """Generator that yields directories containing a valid manifest."""
        for root in roots:
            if root.exists():
                for manifest in root.rglob(MANIFEST_FILENAME):
                    yield manifest.parent

    def _get_managed_paths(self) -> Iterable[Path]:
        """Yields all directories and files managed by the orchestrator."""
        yield from [
            self.exec_ctx.active_path,
            self.exec_ctx.data_path,
            self.exec_ctx.signal_path,
            self.exec_ctx.state_path,
            self.exec_ctx.failed_path,
            self.exec_ctx.workspace_dir / "HOLD",
            self.exec_ctx.workspace_dir / ".cache",
            self.exec_ctx.workspace_dir / "logs",
        ]
        if self.exec_ctx.lock_file.exists():
            yield self.exec_ctx.lock_file

    def _clean_all(self, dry_run: bool) -> None:
        """Wipes the entire local workspace."""
        for target in self._get_managed_paths():
            self._delete_path(target, dry_run)

    def _delete_path(self, path: Path, dry_run: bool) -> None:
        """Helper to delete a file or directory with dry-run support."""
        if not path.exists():
            return

        # SAFETY: Never allow the janitor to delete the root workspace dir itself.
        if path == self.exec_ctx.workspace_dir:
            LOG.error(f"Janitor blocked from deleting workspace root: {path}")
            return

        if dry_run:
            LOG.info(f"[DRY RUN] Would delete: {path}")
        else:
            try:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
                LOG.info(f"Deleted: {path}")
            except Exception as e:
                LOG.error(f"Failed to delete {path}: {e}")

    def _purge_task_by_folder(self, folder: Path, reason: str, dry_run: bool) -> bool:
        """Unified helper to log, transition state, and delete a task folder."""
        if dry_run:
            LOG.info(f"[DRY RUN] Would purge task: {folder.name} ({reason})")
            return True

        # SAFETY: Check if the task is currently active in Ray
        # We check the TaskManager's active registry to prevent deleting folders under a running worker
        active_run_ids = {
            TaskRef.from_str(v).run_id for v in self.active_tasks_fn().values()
        }
        if folder.name in active_run_ids:
            LOG.debug(f"Skipping purge for active task: {folder.name}")
            return False

        try:
            task = Task.from_folder(folder, self.exec_ctx)
            LOG.warning(f"Purging task: {task.run_id} ({reason})")

            # Trigger state machine if this is an expiry-related purge
            if "Expired" in reason and task.manifest.status != ExecutionStatus.EXPIRED:
                ExpiredState().on_enter(
                    task, data={"reason": f"JANITOR_REAP: {reason}"}
                )

            self.cleanup_task(task)
            return True
        except Exception as e:
            LOG.error(f"Failed to purge {folder}: {e}")
            return False

    def cleanup_task(self, task: Task) -> None:
        """
        Standardizes terminal cleanup of a task using the CleanupCoordinator.
        """
        # Coordinate data and metadata cleanup via policies
        CleanupCoordinator().apply(task)
        self._purge_orphaned_config(task.id, task.run_id)

    def _purge_orphaned_config(self, task_id: str, run_id: str) -> None:
        """Removes the legacy orphaned config file in the active root if it exists."""
        prefix = f"{task_id}:{run_id}"
        orphaned_config = self.exec_ctx.active_path / f"{prefix}_{CONFIG_FILENAME}"
        if orphaned_config.exists():
            orphaned_config.unlink()
            LOG.debug("Purged orphaned config file", file=orphaned_config.name)

    def _clean_expired(self, dry_run: bool) -> None:
        """Scans active workspace for manifests and purges those past their TTL."""
        now, count = time.time(), 0
        for folder in self._discover_task_folders([self.exec_ctx.active_path]):
            try:
                ctx = load_task_context(folder)
                if ctx.expires_at and ctx.expires_at < now:
                    ts = pendulum.from_timestamp(ctx.expires_at).in_tz(
                        self.exec_ctx.timezone
                    )
                    reason = f"Expired at {ts.to_datetime_string()}"
                    if self._purge_task_by_folder(folder, reason, dry_run):
                        count += 1
            except Exception:
                continue
        LOG.info(f"Expiration sweep done. Purged: {count}")

    def _clean_run_id(self, run_id: str, dry_run: bool) -> None:
        """Locates and purges a specific run_id across active and quarantined zones."""
        count = 0
        for folder in self._discover_task_folders(self._get_search_roots()):
            if folder.name == run_id:
                if self._purge_task_by_folder(folder, "Targeted Cleanup", dry_run):
                    count += 1
        if not count:
            LOG.warning(f"Run ID {run_id} not found in managed zones.")

    def _clean_older_than(self, days: int, dry_run: bool) -> None:
        """Purges any task folder older than X days, regardless of manifest TTL."""
        cutoff, count = time.time() - (days * 86400), 0
        for folder in self._discover_task_folders(self._get_search_roots()):
            if folder.stat().st_mtime < cutoff:
                if self._purge_task_by_folder(
                    folder, f"Older than {days} days", dry_run
                ):
                    count += 1
        LOG.info(f"Age sweep done. Purged: {count}")


# TODO: Can we add a 'dry_run' flag to the Janitor class to allow simulating expiry sweeps without deleting files?
# TODO: How can we implement a 'dry_run' flag in the Janitor class that logs physical purges without deleting files?
# TODO: How can we implement a 'DRY_RUN' flag in the Janitor class so I can verify these eviction rules in the logs without actually deleting data?
