from datetime import datetime, timedelta
from pathlib import Path

import msgspec
from apps.ingestion.src.core.models.task import Task, TaskSignal
from apps.ingestion.src.core.models.task.manifest import ArchivePayload
from apps.ingestion.src.services.base import Archive
from apps.ingestion.src.services.factory import ServiceFactory
from libs.utils.dates import get_current_timestamp
from loguru import logger

from .base import ExecutionStage
from .enums import StageName

LOG = logger


class ArchiveStage(ExecutionStage):
    name = StageName.ARCHIVE.label
    manifest: ArchivePayload
    service: Archive

    def pre_flight(self, task: "Task") -> None:
        """Verify source connectivity from the execution node."""
        task_ctx = task.context
        if task_ctx.archive.enabled and task_ctx.archive.config:
            self.service = ServiceFactory.get_service(
                service_type=str(task_ctx.archive.type), **task_ctx.archive.config
            )
            print(f"Initialized Archive service: {self.service}")
            try:
                # Do not use the S3 URL here, just list buckets to see what's visible
                visible_buckets = [b['Name'] for b in self.service.client.fs.call_s3('list_buckets')['Buckets']]
                print(f"WORKER DEBUG: Visible buckets: {visible_buckets}")
                print(f"WORKER DEBUG: {self.service.client.fs.ls(self.service.url)}")
            except Exception as e:
                print(f"WORKER DEBUG: Failed to connect: {e}")
                
                
            # For S3/Cloud storage, prefixes don't "exist" until they contain files.
            # We verify the bucket/root exists to confirm connectivity and permissions.
            check_url = self.service.url
            if "://" in check_url:
                # Extract protocol and bucket: s3://my-bucket/prefix -> s3://my-bucket
                parts = check_url.split("/")
                check_url = "/".join(parts[:3])

            if not self.service.client.exists(check_url):
                raise ConnectionError(
                    f"Archive pre-flight failed: Root destination '{check_url}' is unreachable or does not exist."
                )

    def execute(self, task: Task) -> str:
        """
        Decision: The 'Zero-Footprint' Protocol.
        We preserve the audit trail and the output data in long-term storage
        while reclaiming high-speed local disk space.
        """

        task_ctx = task.context
        LOG.info(
            "ArchiveStage started",
            job_id=task.job_id,
            run_id=task.run_id,
        )

        start_ts = get_current_timestamp(strip_tz=True).isoformat(sep=" ")

        try:
            # 1. OPTIONAL ARCHIVAL
            # Subject to privacy requirements defined in job_config
            final_archive_path = None
            if task_ctx.archive.enabled:
                if not task_ctx.archive.base_path:
                    raise ValueError("Archive base path is required for archival.")
                # Establish the root archival path for this specific run
                final_archive_path = (
                    Path(task_ctx.archive.base_path)
                    / task.job_id
                    / task.partition_date
                    / task.run_id
                )

                # 1. Archive Parquet Files
                # We archive data from the 'data/' vault.
                # This includes both the Extract (Sanitized) and Transform results.
                self._archive_parquet_data(self.service, task, str(final_archive_path))

            # 2. Finalize Timing
            end_ts = get_current_timestamp(strip_tz=True)

            # 3. FINALIZE CANONICAL PAYLOAD
            payload = ArchivePayload(
                start_timestamp=str(start_ts),
                end_timestamp=end_ts.isoformat(sep=" "),
                cleanup_verified=False,  # Physical cleanup deferred to Janitor
                archival_path=str(final_archive_path) if final_archive_path else None,
                retention_expiry=self._calculate_expiry(task, end_ts),
            )
            results = msgspec.to_builtins(payload)

            LOG.debug(
                "Finalizing manifest with ArchivePayload",
                job_id=task.job_id,
                payload=results,
            )
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
            LOG.exception("ArchiveStage failed during cleanup")
            self.finalize(task, exception=e)
            raise

    def _archive_parquet_data(
        self, object_store: Archive, task: Task, archive_root: str
    ) -> None:
        """
        Decision: Move files to the Archive location defined in the Context.
        Standardizing on: archive/{job_id}/{run_id}/{stage}/
        """

        # We loop through the stages we want to keep
        for stage in [StageName.EXTRACT, StageName.TRANSFORM]:
            # Using .label ensures we use the case-folded string expected by the filesystem
            label = stage.label
            # Follow the active symlink to find the physical data
            src_folder = task.folder.resolve() / label
            if src_folder.exists():
                dest_folder = f"{archive_root}/{label}"
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
        # retention_days is nested within the archive configuration
        retention_days = getattr(job.context.archive, "retention_days", 2555) or 2555
        return (end_timestamp + timedelta(days=retention_days)).date().isoformat()
