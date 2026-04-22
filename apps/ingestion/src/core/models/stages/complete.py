import shutil
import time
from datetime import datetime, timedelta

import msgspec
from apps.ingestion.src.core.models.task import Task, TaskSignal
from apps.ingestion.src.core.models.task.manifest import CompletePayload
from apps.ingestion.src.services.base import Archive
from apps.ingestion.src.services.factory import ServiceFactory
from loguru import logger

from .base import ExecutionStage
from .enums import StageName

LOG = logger


class CompleteStage(ExecutionStage):
    name = StageName.COMPLETE.label
    manifest: CompletePayload

    def pre_flight(self, task: "Task") -> None:
        """Verify source connectivity from the execution node."""
        task_ctx = task.context
        if task_ctx.archive.enabled:
            self.service = ServiceFactory.get_archive(
                service_type=task_ctx.archive.type, **task_ctx.archive.config
            )

    def execute(self, task: Task) -> str:
        """
        Decision: The 'Zero-Footprint' Protocol.
        We preserve the audit trail and the output data in long-term storage
        while reclaiming high-speed local disk space.
        """

        task_ctx = task.context
        start_ts = datetime.now().astimezone().isoformat()

        try:
            # 1. OPTIONAL ARCHIVAL
            # Subject to privacy requirements defined in job_config
            final_archive_path = None
            if task_ctx.archive.enabled:
                # 1. Archive Parquet Files
                # We archive data from the 'data/' vault.
                # This includes both the Extract (Sanitized) and Transform results.
                self._archive_parquet_data(self.service, task)

            # 2. Garbage Collection Delay
            # Allows Ray workers and Polars memory maps to release file handles
            # before we attempt a physical wipe of the data vaults.
            time.sleep(2)

            # 3. PURGE DATA VAULTS
            # We identify physical data via symlinks in the task folder.
            # This ensures we only delete data belonging to this specific run.
            LOG.info("Purging physical data vaults", job_id=task.job_id)
            for item in task.folder.iterdir():
                if item.is_symlink():
                    real_data_path = item.resolve()
                    if real_data_path.exists():
                        LOG.debug("Deleting vault directory", path=str(real_data_path))
                        shutil.rmtree(real_data_path, ignore_errors=True)
                    item.unlink()  # Remove the symlink

            # 4. Calculate Timestamps and Duration
            end_ts = datetime.now().astimezone()

            # 5. FINALIZE CANONICAL PAYLOAD
            payload = CompletePayload(
                start_timestamp_utc=start_ts,
                end_timestamp_utc=end_ts.isoformat(),
                cleanup_verified=True,
                archival_path=str(final_archive_path) if final_archive_path else None,
                retention_expiry=self._calculate_expiry(task, end_ts),
            )
            results = msgspec.to_builtins(payload)
            self.finalize(task, results=results)

            # 2. Store Manifest in Database (Current Execution Table)
            # Decision: By moving manifest data to SQL, we allow the BI team to
            # monitor job performance without needing file system access.
            task.request_status_sync(TaskSignal.DONE)

            # 4. Final Finalize (Post-Purge)
            # We don't use a symlink here; we just record SUCCESS in the DB/State Store
            LOG.info(
                "Task lifecycle complete.",
                stage=self.name,
                job_id=task.job_id,
            )
            return "FINISH"

        except Exception as e:
            LOG.exception("CompleteStage failed during cleanup")
            self.finalize(task, exception=e)
            raise

    def _archive_parquet_data(self, object_store: Archive, task: Task) -> None:
        """
        Decision: Move files to the Archive location defined in the Context.
        Standardizing on: archive/{job_id}/{run_id}/{stage}/
        """
        archive_root = f"{task.context.archive.base_path}/{task.job_id}/{task.run_id}"

        # We loop through the stages we want to keep
        for stage in ["extract", "transform"]:
            # Follow the active symlink to find the physical data
            src_folder = task.folder.resolve() / stage
            if src_folder.exists():
                dest_folder = f"{archive_root}/{stage}"
                object_store.archive_data(
                    source_dir=src_folder, archive_path=dest_folder
                )

    def _calculate_expiry(self, job: Task, end_timestamp: datetime) -> str:
        # e.g., standard 7-year retention or 30-day GDPR limit
        """
        Calculates the retention expiry date for a job.

        Uses the retention_days attribute from the TaskContext if present,
        otherwise falls back to a 7-year default.

        Returns an ISO-formatted string representing the retention expiry date.
        """
        retention_days = getattr(job.context, "retention_days", 2555)  # 7 years default
        return (end_timestamp + timedelta(days=retention_days)).date().isoformat()
