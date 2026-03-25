from datetime import datetime
from typing import TYPE_CHECKING

import msgspec
import structlog

from apps.ingestion.src.core.models.job.manifest import WritePayload
from apps.ingestion.src.core.strategies.load.load import Loader
from apps.ingestion.src.services.factory import ServiceFactory

from .base import JobStep

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.job import Job


LOG = structlog.getLogger(__name__)


class WriteStep(JobStep):
    manifest: WritePayload

    @property
    def name(self) -> str:
        return "write"

    def execute(self, job: "Job") -> str:
        start_ts = datetime.now().astimezone().isoformat()
        job_ctx = job.context

        try:
            # 1. Resolve logical input (The partitioned parquet files)
            source_dir = (job.folder / "transform").resolve()

            # 1. Get the Service (Securely initialized on Ray worker via ServiceFactory)
            service = ServiceFactory.get_sink(
                job_ctx.load.sink_type, **job_ctx.load.sink_config
            )

            LOG.info(
                "Starting load",
                sink_type=job_ctx.load.sink_type,
                target=job_ctx.load.sink_identifier,
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 2. PHASE 1: LOAD TO STAGING
            staging_artifact, rows_loaded = loader.load(
                service=service,
                source_dir=source_dir,
                target_table=job_ctx.load.sink_identifier,
            )

            # 3. Finalize Manifest
            payload = WritePayload(
                sink_identifier=job_ctx.load.sink_identifier,
                sink_type=job_ctx.load.sink_type,
                staging_artifact=staging_artifact,
                rows_inserted=int(rows_loaded),
                partition_col=job_ctx.load.partition_col or "",
                partition_value=job_ctx.load.partition_value or "",
                start_timestamp_utc=start_ts,
            )

            self.finalize(job, results=msgspec.to_builtins(payload))
            LOG.info(
                "Load complete",
                rows=int(rows_loaded),
                staging_artifact=staging_artifact,
            )
            return str(self._transit(job))

        except Exception as exc:
            self.finalize(job, exception=exc)
            raise
