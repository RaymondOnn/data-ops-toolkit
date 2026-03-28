from datetime import datetime
from typing import TYPE_CHECKING

import msgspec
import structlog

from apps.ingestion.src.core.models.job.manifest import WritePayload
from apps.ingestion.src.core.strategies.load.load import Loader
from apps.ingestion.src.services.factory import ServiceFactory

from .base import ExecutionStage
from .enums import StageName

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.job import Task


LOG = structlog.getLogger(__name__)


class WriteStep(ExecutionStage):
    name = StageName.WRITE.label
    manifest: WritePayload

    def execute(self, job: "Task") -> str:
        start_ts = datetime.now().astimezone().isoformat()
        task_ctx = job.context

        try:
            # 1. Resolve logical input (The partitioned parquet files)
            source_dir = (job.folder / "transform").resolve()

            # Verify source_dir actually contains files before proceeding
            if not any(source_dir.glob("*.parquet")):
                raise FileNotFoundError(
                    f"No parquet files found in transformed data directory: {source_dir}"
                )

            # 1. Get the Service (Securely initialized on Ray worker via ServiceFactory)
            service = ServiceFactory.get_sink(
                task_ctx.load.sink_type, **task_ctx.load.sink_config
            )

            LOG.info(
                "Starting load",
                stage=self.name,
                sink_type=task_ctx.load.sink_type,
                target=task_ctx.load.sink_identifier,
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 2. PHASE 1: LOAD TO STAGING
            staging_artifact, rows_loaded = loader.load(
                service=service,
                source_dir=source_dir,
                target_table=task_ctx.load.sink_identifier,
                partition_col=task_ctx.load.partition_col,
                partition_val=task_ctx.load.partition_value,
            )

            # 3. Finalize Manifest
            payload = WritePayload(
                sink_identifier=task_ctx.load.sink_identifier,
                sink_type=task_ctx.load.sink_type,
                staging_artifact=staging_artifact,
                rows_inserted=int(rows_loaded),
                partition_col=task_ctx.load.partition_col or "",
                partition_value=task_ctx.load.partition_value or "",
                start_timestamp_utc=start_ts,
            )

            self.finalize(job, results=msgspec.to_builtins(payload))
            LOG.info(
                "Load complete",
                stage=self.name,
                rows=int(rows_loaded),
                staging_artifact=staging_artifact,
            )
            return str(self._transit(job))

        except Exception as exc:
            self.finalize(job, exception=exc)
            raise
            raise
            raise
            raise
            raise
            raise
            raise
            raise
            raise
            raise
