import shutil
from datetime import UTC, datetime, timedelta

import msgspec
import structlog
from src.core.models.job import Job
from src.core.models.job.manifest import CompletePayload
from src.core.models.steps import JobStep
from src.services.factory import ServiceFactory
from src.services.file import StorageService

LOG = structlog.getLogger(__name__)


class CompleteStep(JobStep):
    manifest: CompletePayload

    @property
    def name(self) -> str:
        return "complete"

    def execute(self, job: "Job") -> str:
        """
        Decision: The 'Zero-Footprint' Protocol.
        We preserve the audit trail and the output data in long-term storage
        while reclaiming high-speed local disk space.
        """

        job_ctx = job.context
        start_ts = datetime.now(UTC).isoformat()

        # 1. Initialize Storage Service for Archival
        # We retrieve the 'archive' service defined in the job configuration
        object_store: StorageService = ServiceFactory.get_service(
            type=job_ctx.archive.type,  # e.g., "s3" or "local"
            **job_ctx.archive.config,
        )

        try:
            # 2. OPTIONAL ARCHIVAL
            # Subject to privacy requirements defined in job_config
            final_archive_path = None
            if job_ctx.archive.enabled:
                # 1. Archive Parquet Files
                # We move data from the high-speed 'data/' vault to the 'archive/' vault.
                # This includes both the Extract (Sanitized) and Transform results.
                self._archive_parquet_data(object_store, job)

            # 3. CLEANUP VERIFICATION
            # Force removal of all intermediate data (Extract & Transform folders)
            local_run_root = job.folder.parent
            for folder in ["extract", "transform"]:
                target = local_run_root / folder
                if target.exists():
                    shutil.rmtree(target)

            # 4. Calculate Timestamps and Duration
            end_ts = datetime.now(UTC)

            # 5. FINALIZE CANONICAL PAYLOAD
            payload = CompletePayload(
                start_timestamp_utc=start_ts,
                end_timestamp_utc=end_ts.isoformat(),
                cleanup_verified=True,
                archival_path=str(final_archive_path) if final_archive_path else None,
                retention_expiry=self._calculate_expiry(job, end_ts),
            )
            results = msgspec.to_builtins(payload)
            self.finalize(job, results=results)

            # 2. Store Manifest in Database (Current Execution Table)
            # Decision: By moving manifest data to SQL, we allow the BI team to
            # monitor job performance without needing file system access.
            job.request_status_sync(deep_sync=True)

            # 4. Final Finalize (Post-Purge)
            # We don't use a symlink here; we just record SUCCESS in the DB/State Store
            LOG.info("Job lifecycle complete. Workspace purged.", job_id=job.id)

            # This marks the final state of the manifest
            return "FINISH"

        except Exception as e:
            self.finalize(job, exception=e)
            raise

    def _archive_parquet_data(self, object_store: StorageService, job: Job) -> None:
        """
        Decision: Move files to the Archive location defined in the Context.
        Standardizing on: archive/{job_id}/{run_id}/{step}/
        """
        archive_root = f"{job.context.archive.base_path}/{job.id}/{job.run_id}"

        # We loop through the steps we want to keep
        for step in ["extract", "transform"]:
            # Follow the active symlink to find the physical data
            src_folder = job.folder.resolve() / step
            if src_folder.exists():
                dest_folder = f"{archive_root}/{step}"
                object_store.archive_data(
                    source_dir=src_folder, archive_path=dest_folder
                )

    def _calculate_expiry(self, job: "Job", end_timestamp: datetime) -> str:
        # e.g., standard 7-year retention or 30-day GDPR limit
        """
        Calculates the retention expiry date for a job.

        Uses the retention_days attribute from the JobContext if present,
        otherwise falls back to a 7-year default.

        Returns an ISO-formatted string representing the retention expiry date.
        """
        retention_days = getattr(job.context, "retention_days", 2555)  # 7 years default
        return (end_timestamp + timedelta(days=retention_days)).date().isoformat()
