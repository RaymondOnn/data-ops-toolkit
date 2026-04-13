from datetime import datetime
from typing import TYPE_CHECKING

import msgspec
import structlog
from apps.ingestion.src.core.models.job.manifest import PublishPayload
from apps.ingestion.src.core.strategies.load.load import Loader, WriteContext
from apps.ingestion.src.services.factory import ServiceFactory

from .base import ExecutionStage
from .enums import StageName

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.job import Task
    from apps.ingestion.src.services.base import Sink


LOG = structlog.getLogger(__name__)


class PublishStage(ExecutionStage):
    """
    Decision: The PublishStep makes the data 'Public'.
    We use the context to identify the target 'Prod' table vs 'Staging' table.
    """

    name = StageName.PUBLISH.label

    manifest: PublishPayload
    service: Sink

    def pre_flight(self, task: "Task") -> None:
        """
        Bypass global pre-flight checks (like disk pressure).
        Publishing is a priority stage to reclaim resources.
        """
        pass

    def execute(self, task: "Task") -> str:
        self.pre_flight(task)

        task_ctx = task.context
        start_ts = datetime.now().astimezone().isoformat()

        try:
            # 1. Initialize Service & Check Connectivity
            # (This logic is the 'new' pre-flight abstraction)
            self.service = ServiceFactory.get_sink(
                task_ctx.load.sink_type, **task_ctx.load.sink_config
            )

            write_meta = task.manifest.write
            if not write_meta or not write_meta.staging_artifact:
                LOG.warning("Missing write metadata. Rewinding to WRITE stage.")
                return self._rewind(task, StageName.WRITE)

            # 2. Check Staging Artifact (Self-Healing Rewind)
            staging_id = write_meta.staging_artifact
            if not self.service.exists(staging_id):
                LOG.warning(
                    "Staging artifact lost. Rewinding to WRITE stage.",
                    artifact=staging_id,
                )
                return self._rewind(task, StageName.WRITE)

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 3. Create Context
            context = WriteContext(
                sink_identifier=task_ctx.load.sink_identifier,
                partition_col=task_ctx.load.partition_col,
                partition_value=task_ctx.load.partition_value,
            )

            LOG.info(
                "Promoting to production",
                stage=self.name,
                target=task_ctx.load.sink_identifier,
                staging=write_meta.staging_artifact,
            )

            # 2. FINISH THE JOB
            # Move from staging to production
            loader.promote(
                service=self.service,
                staging_identifier=write_meta.staging_artifact,
                write_ctx=context,
            )

            # 3. PAYLOAD: The 'Success Receipt'
            # Get count from previous write stage if available
            final_count = 0
            if task.manifest.write:
                final_count = task.manifest.write.rows_inserted

            payload = PublishPayload(
                final_destination=task_ctx.load.sink_identifier,
                final_count=final_count,
                start_timestamp_utc=start_ts,
            )

            self.finalize(task, results=msgspec.to_builtins(payload))
            LOG.info(
                "Publish complete", stage=self.name, table=task_ctx.load.sink_identifier
            )
            return str(self._transit(task))

        except Exception as e:
            self.finalize(task, exception=e)
            raise
