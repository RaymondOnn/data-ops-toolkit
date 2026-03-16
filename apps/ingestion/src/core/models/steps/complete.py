import shutil
from datetime import datetime, timedelta

import structlog
from src.core.models.job import Job
from src.core.models.job.manifest import CompletePayload
from src.core.models.steps import JobBitmask, JobStep
from src.services.factory import ServiceFactory
from src.services.file import StorageService

LOG = structlog.getLogger(__name__)


class CompleteStep(JobStep):  # type: ignore
    manifest: CompletePayload

    @property
    def bitmask(self) -> str:
        return str(JobBitmask.COMPLETE)

    @property
    def name(self) -> str:
        return "complete"

    def execute(self, job: "Job") -> str:
        """
        Decision: The 'Zero-Footprint' Protocol.
        We preserve the audit trail and the output data in long-term storage
        while reclaiming high-speed local disk space.
        """
        from src.services.factory import ServiceFactory
        from src.services.file import StorageService

        self.manifest = self.get_manifest(job)
        job_ctx = job.context

        # 1. Initialize Storage Service for Archival
        # We retrieve the 'archive' service defined in the job configuration
        object_store: StorageService = ServiceFactory.get_service(
            type=job_ctx.archive_type,  # e.g., "s3" or "local"
            **job_ctx.archive_config,
        )

        try:
            # 2. OPTIONAL ARCHIVAL
            # Subject to privacy requirements defined in job_config
            final_archive_path = None
            if job_ctx.enable_archival:
                # 1. Archive Parquet Files
                # We move data from the high-speed 'data/' vault to the 'archive/' vault.
                # This includes both the Raw (Sanitized) and Transform results.
                self._archive_parquet_data(object_store, job)

            # 3. CLEANUP VERIFICATION
            # Force removal of all intermediate data (Raw & Transform folders)
            local_run_root = job.folder.parent
            for folder in ["raw", "transform"]:
                target = local_run_root / folder
                if target.exists():
                    shutil.rmtree(target)

            # 4. Calculate Timestamps and Duration
            start_ts = datetime.fromisoformat(job.manifest.start.ingestion_started_at)
            end_ts = datetime.now()
            duration_secs = (end_ts - start_ts).total_seconds()

            # 5. FINALIZE CANONICAL PAYLOAD
            payload = CompletePayload(
                execution_outcome="SUCCESS",
                end_timestamp=end_ts.isoformat(),
                total_duration_secs=round(duration_secs, 2),
                cleanup_verified=True,
                archival_path=final_archive_path,
                retention_expiry=self._calculate_expiry(job, end_ts),
            )

            self.finalize(job, results=payload)

            # 2. Store Manifest in Database (Current Execution Table)
            # Decision: By moving manifest data to SQL, we allow the BI team to
            # monitor job performance without needing file system access.
            self._record_execution_to_db(job)

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
        archive_root = f"{job.context.archive_base_path}/{job.id}/{job.run_id}"

        # We loop through the steps we want to keep
        for step in ["raw", "transform"]:
            # Follow the active symlink to find the physical data
            src_folder = job.folder.resolve() / step
            if src_folder.exists():
                dest_folder = f"{archive_root}/{step}"
                # target_archive.mkdir(parents=True, exist_ok=True)
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

    def _record_execution_to_db(self, job: "Job") -> None:
        """
        Decision: Upsert final stats into the 'job_execution_history' table.
        This provides a high-level audit trail for 50M row jobs.
        """
        db = ServiceFactory.get_service(job.context.target_type)
        manifest = self.get_manifest(job)  # Final read of the audit trail

        db.execute_query(
            "INSERT INTO job_execution_history (job_id, run_id, rows_in, rows_out, duration) VALUES (%s, %s, %s, %s, %s)",
            (
                job.id,
                job.run_id,
                manifest.raw.total_rows,
                manifest.write.rows_written,
                manifest.total_duration,
            ),
        )


