from datetime import timedelta

from apps.ingestion.src.core.contexts import ArchiveConfig
from apps.ingestion.src.core.models.task import Task, TaskSignal
from apps.ingestion.src.core.models.task.manifest import ArchivePayload
from apps.ingestion.src.services.factory import ServiceFactory
from libs.utils.dates import current_timestamp
from loguru import logger

from .base import ExecutionStage
from .enums import Stage
from .utils import stage

LOG = logger
DEFAULT_RETENTION_DAYS = 2555  # 7 years


@stage(Stage.ARCHIVE.value)
class ArchiveStage(ExecutionStage[ArchiveConfig]):
    requires_disk_space: bool = False
    config_attribute = "archive"

    def pre_flight(self, task: Task) -> None:
        """Verify archive destination is reachable."""
        super().pre_flight(task)

        if not self.config.enabled:
            return

        if not self.config.enabled or not self.config.connection:
            return

        # Validate bucket/root exists (not just prefix)
        check_url = self.config.base_path
        if "://" in check_url:
            # Extract protocol + bucket: s3://my-bucket/prefix -> s3://my-bucket
            check_url = "/".join(check_url.split("/")[:3])

        self.archive = ServiceFactory.get_archive(**self.config.connection)
        if not self.archive.exists(check_url):
            raise ConnectionError(f"Archive destination unreachable: {check_url}")

    def _execute(self, task: Task) -> str:
        """Execute archival and cleanup."""
        if not self.config.enabled:
            return self._next_stage()

        LOG.info(f"Archiving job {task.job_id}, run {task.run_id}")
        start_time = current_timestamp(naive=True).isoformat(sep=" ")

        try:
            archive_path = self._archive_data(task)
            expiry_date = self._calculate_expiry()

            payload = ArchivePayload(
                start_time=start_time,
                end_time=current_timestamp(naive=True).isoformat(sep=" "),
                archive_path=archive_path,
                retention_expiry=expiry_date,
            )

            self.checkpoint(task, payload=payload)
            task.send_signal(TaskSignal.DONE)

            LOG.info(f"Archive complete for {task.run_id}")
            return "FINISH"

        except Exception as e:
            LOG.exception("Archive failed")
            self.checkpoint(task, error=e)
            raise

    def _archive_data(self, task: Task) -> str | None:
        """Archive data to long-term storage."""
        if not self.config.enabled:
            return None

        # Build archive path: base/job_id/partition_date/run_id
        archive_root = task.get_archive_path(base_path=self.config.base_path)

        # Archive each stage's data
        for stage_name in [Stage.EXTRACT.value, Stage.TRANSFORM.value]:
            src = task.workspace.path / stage_name
            if src.exists():
                self.archive.store(source=src, dest=f"{archive_root}/{stage_name}")
                LOG.debug(f"Archived {stage_name} data to {archive_root}/{stage_name}")

        return archive_root

    def _calculate_expiry(self) -> str:
        # e.g., standard 7-year retention or 30-day GDPR limit
        """
        Calculates the retention expiry date for a job.

        Uses the retention_days attribute from the TaskContext if present,
        otherwise falls back to a 7-year default.

        Returns an ISO-formatted string representing the retention expiry date.
        """

        days = (
            self.config.retention_days
            if self.config.enabled
            else DEFAULT_RETENTION_DAYS
        )
        expiry = current_timestamp(naive=True) + timedelta(days=days)
        return expiry.date().isoformat()
