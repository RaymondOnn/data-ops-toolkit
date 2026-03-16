import time
from datetime import datetime

import structlog
from src.core.strategies.load.load import Loader, WriteContext
from src.core.models.job import Job
from src.core.models.job.manifest import PublishPayload
from src.core.models.steps import JobBitmask, JobStep
from src.services.factory import ServiceFactory

LOG = structlog.getLogger(__name__)


class PublishStep(JobStep):  # type: ignore
    """
    Decision: The PublishStep makes the data 'Public'.
    We use the context to identify the target 'Prod' table vs 'Staging' table.
    """

    manifest: PublishPayload

    @property
    def bitmask(self) -> str:
        return str(JobBitmask.PUBLISH)

    @property
    def name(self) -> str:
        return "publish"

    def execute(self, job: "Job") -> str:
        start_time = time.perf_counter()
        job_ctx = job.context


        try:
            manifest = self.get_manifest(job)
            write_meta = manifest.write
            if not write_meta:
                raise ValueError("Write metadata not found in manifest.")

            # 1. Get the Service (Securely initialized on Ray worker via ServiceFactory)
            service = ServiceFactory.get_service(
                job_ctx.sink_type, **job_ctx.sink_config
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 3. Create Context
            context = WriteContext(
                target=job_ctx.target_table,
                partition_col=job_ctx.partition_col,
                partition_value=job_ctx.partition_value,
            )

            # 2. FINISH THE JOB
            # Move from staging to production
            loader.promote(
                service=service,
                staging_info=write_meta.staging_artifact,
                write_ctx=context,
            )
            LOG.info(
                "Job Published", job_id=job.id, table=job_ctx.target_destination
            )

            # 3. PAYLOAD: The 'Success Receipt'
            duration_ms = round(time.time() - start_time, 2)
            payload = PublishPayload(
                step_outcome="COMPLETED",
                final_destination=job_ctx.target_identifier,
                promotion_duration_secs=duration_ms,
                completed_at=datetime.now().isoformat(),
            )

            self.finalize(job, results=payload)
            return str(self._transit(job))  # This likely triggers 'FINISH'

        except Exception as e:
            self.finalize(job, exception=e)
            raise
