from datetime import UTC, datetime
from typing import TYPE_CHECKING

import msgspec
import structlog
from src.core.models.job.manifest import WritePayload
from src.core.models.steps import JobStep
from src.core.strategies.load.load import Loader
from src.services.factory import ServiceFactory

if TYPE_CHECKING:
    from src.core.models.job import Job


LOG = structlog.getLogger(__name__)


class WriteStep(JobStep):
    manifest: WritePayload

    @property
    def name(self) -> str:
        return "write"

    def execute(self, job: "Job") -> str:
        start_ts = datetime.now(UTC).isoformat()
        job_ctx = job.context

        try:
            # 1. Resolve logical input (The partitioned parquet files)
            source_dir = (job.folder / "transform").resolve()

            # 1. Get the Service (Securely initialized on Ray worker via ServiceFactory)
            service = ServiceFactory.get_service(
                job_ctx.load.sink_type, **job_ctx.load.sink_config
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 2. PHASE 1: LOAD TO STAGING
            staging_results = loader.load(
                service=service,
                source_dir=source_dir,
                target_table=job_ctx.load.sink_identifier,
            )

            # 3. Finalize Manifest
            payload = WritePayload(
                sink_identifier=job_ctx.load.sink_identifier,
                sink_type=job_ctx.load.sink_type,
                staging_artifact=str(
                    staging_results.staging_path or staging_results.staging_table
                ),
                rows_inserted=staging_results.rows,
                partition_col=job_ctx.load.partition_col or "",
                partition_value=job_ctx.load.partition_value or "",
                start_timestamp_utc=start_ts,
            )

            self.finalize(job, results=msgspec.to_builtins(payload))
            return str(self._transit(job))

        except Exception as exc:
            self.finalize(job, exception=exc)
            raise
