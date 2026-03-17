import time
import traceback

import structlog # type: ignore

from src.core.strategies.load.load import Loader
from src.core.models.job import Job
from src.core.models.steps import JobBitmask, JobStep
from src.core.models.job.manifest import ErrorPayload, WritePayload
from src.services.factory import ServiceFactory
from src.utils.constants import JOB_STEPS_BASE_DIR

LOG = structlog.getLogger(__name__)


class WriteStep(JobStep):  # type: ignore
    manifest: WritePayload

    @property
    def bitmask(self) -> str:
        return str(JobBitmask.WRITE)

    @property
    def name(self) -> str:
        return "write"

    def execute(self, job: "Job") -> str:
        start_time = time.perf_counter()
        job_ctx = job.context


        try:
            # 1. Resolve logical input (The partitioned parquet files)
            source_dir = (job.folder / "transform").resolve()

            # 1. Get the Service (Securely initialized on Ray worker via ServiceFactory)
            service = ServiceFactory.get_service(
                job_ctx.sink_type, **job_ctx.sink_config
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 2. PHASE 1: LOAD TO STAGING
            staging_results = loader.load(
                service=service,
                source_dir=source_dir,
                target_table=job_ctx.target_destination,
            )

            # 3. Finalize Manifest
            payload = WritePayload(
                step_outcome="COMPLETED",
                target_identifier=job_ctx.target_destination,
                sink_type=job_ctx.destination_type,
                staging_artifact=(
                    staging_results.staging_path or staging_results.staging_table,
                ),
                rows_inserted=staging_results.rows,
                partition_col=job_ctx.partition_col,
                partition_value=job_ctx.partition_value,
                db_connection_id=service.connection_id,
                duration_secs=int((time.perf_counter() - start_time) * 1000),
            )

            self.finalize(job, results=payload)
            return str(self._transit(job))

        except Exception as exc:
            payload = ErrorPayload(
                step_name=self.name,
                error_type=type(exc).__name__,
                message=str(exc),
                stack_trace=traceback.format_exc(),
                is_transient=isinstance(
                    exc, (requests.RequestException, ConnectionError)
                ),
            )
            self.finalize(job, exception=payload)
            raise
