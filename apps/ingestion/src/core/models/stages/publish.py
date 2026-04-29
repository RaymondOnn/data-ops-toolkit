from datetime import datetime
from typing import TYPE_CHECKING

import msgspec
from loguru import logger

from apps.ingestion.src.core.models.task.manifest import PublishPayload
from apps.ingestion.src.core.strategies.load.load import LoadContext, Loader
from apps.ingestion.src.services.base import Sink
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.exceptions import RetryTask, RewindTask

from .base import ExecutionStage
from .enums import StageName

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task


LOG = logger


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
        # 1. Initialize Service & Check Connectivity
        # (This logic is the 'new' pre-flight abstraction)
        task_ctx = task.context
        self.service = ServiceFactory.get_sink(
            task_ctx.load.sink_type, **task_ctx.load.sink_config
        )

        # 2. Check Staging Artifact (Self-Healing Rewind)
        write_meta = task.manifest.write
        if not write_meta or not write_meta.staging_artifact:
            LOG.warning("Missing write metadata. Rewinding to WRITE stage.")
            raise RewindTask(
                target_stage=StageName.WRITE.label,
                reason="Staging artifact missing for publication.",
            )

    def execute(self, task: "Task"):
        start_ts = datetime.now().astimezone().isoformat()

        try:
            task_ctx = task.context
            write_meta = task.manifest.write
            if not write_meta:
                LOG.error("Write metadata is required for Publish stage.")
                raise ValueError("Write metadata is required for Publish stage.")

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 3. Create Context
            context = LoadContext(
                sink_identifier=task_ctx.load.sink_identifier,
                partition_col=task_ctx.load.partition_col,
                partition_value=task_ctx.load.partition_value,
                expected_count=write_meta.rows_inserted,
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
                load_ctx=context,
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
