from datetime import datetime
from typing import TYPE_CHECKING

import msgspec
import structlog
from apps.ingestion.src.core.models.job.manifest import PublishPayload
from apps.ingestion.src.core.strategies.load.load import Loader, WriteContext
from apps.ingestion.src.services.factory import ServiceFactory

from .base import JobStep
from .enums import JobSteps

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.job import Job


LOG = structlog.getLogger(__name__)


class PublishStep(JobStep):
    """
    Decision: The PublishStep makes the data 'Public'.
    We use the context to identify the target 'Prod' table vs 'Staging' table.
    """

    name = JobSteps.PUBLISH.label

    manifest: PublishPayload

    def execute(self, job: "Job") -> str:
        job_ctx = job.context
        start_ts = datetime.now().astimezone().isoformat()

        try:
            write_meta = job.manifest.write
            if not write_meta:
                raise ValueError("Write metadata not found in manifest.")

            # 1. Get the Service (Securely initialized on Ray worker via ServiceFactory)
            service = ServiceFactory.get_sink(
                job_ctx.load.sink_type, **job_ctx.load.sink_config
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 3. Create Context
            context = WriteContext(
                sink_identifier=job_ctx.load.sink_identifier,
                partition_col=job_ctx.load.partition_col,
                partition_value=job_ctx.load.partition_value,
            )

            LOG.info(
                "Promoting to production",
                step=self.name,
                target=job_ctx.load.sink_identifier,
                staging=write_meta.staging_artifact,
            )

            # 2. FINISH THE JOB
            # Move from staging to production
            loader.promote(
                service=service,
                staging_identifier=write_meta.staging_artifact,
                write_ctx=context,
            )

            # 3. PAYLOAD: The 'Success Receipt'
            # Get count from previous write step if available
            final_count = 0
            if job.manifest.write:
                final_count = job.manifest.write.rows_inserted

            payload = PublishPayload(
                final_destination=job_ctx.load.sink_identifier,
                final_count=final_count,
                start_timestamp_utc=start_ts,
            )

            self.finalize(job, results=msgspec.to_builtins(payload))
            LOG.info(
                "Publish complete", step=self.name, table=job_ctx.load.sink_identifier
            )
            return str(self._transit(job))

        except Exception as e:
            self.finalize(job, exception=e)
            raise
