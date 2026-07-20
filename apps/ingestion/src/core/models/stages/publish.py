"""Publish stage for promoting staged data to production."""

from typing import TYPE_CHECKING

from apps.ingestion.src.core.contexts import LoadConfig
from apps.ingestion.src.core.models.task.manifest import PublishPayload
from apps.ingestion.src.core.strategies.load.load import LoadContext, LoaderFactory
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.exceptions import RollbackRequired
from libs.utils.dates import current_timestamp
from loguru import logger

from .base import ExecutionStage
from .enums import Stage
from .utils import stage

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task
    from apps.ingestion.src.core.models.task.manifest import WritePayload


LOG = logger


@stage(Stage.PUBLISH.value)
class PublishStage(ExecutionStage[LoadConfig]):
    """Stage for promoting staged data to production.

    This stage handles the "Promotion" phase of the load strategy, moving data
    from a temporary staging area (created in the WRITE stage) to the final
    production destination.
    """

    requires_disk_space: bool = False
    config_attribute = "write"

    write: "WritePayload"

    def pre_flight(self, task: "Task") -> None:
        """Initialize sink and verify staging artifact exists.

        Args:
            task (Task): The current task instance being executed.

        Raises:
            RollbackRequired: If the WRITE stage manifest or the staging
                artifact identifier is missing.

        Notes:
            We explicitly check for the `staging_artifact` here to prevent
            the loader from attempting a promotion on a non-existent or
            corrupted staging state. If missing, we rewind to WRITE to
            re-attempt the staging process.
        """
        super().pre_flight(task)
        self.sink = ServiceFactory.get_sink(**self.config.connection)

        # Verify write metadata exists
        if task.manifest.write is None:
            raise RollbackRequired(Stage.WRITE.value, "Missing write metadata")
        self.write = task.manifest.write

        if not self.write.staging_artifact:
            LOG.warning("Missing staging artifact, rewinding to WRITE")
            raise RollbackRequired(Stage.WRITE.value, "Staging artifact missing")

    def _execute(self, task: "Task") -> str:
        """Promote staged data to production.

        Args:
            task (Task): The task instance containing the manifest and context.

        Returns:
            str: The name of the next stage (usually ARCHIVE) or 'FINISH'.

        Raises:
            RollbackRequired: If the write manifest is unexpectedly None.
            Exception: Propagates any underlying database or promotion errors.

        Notes:
            - We use the `loader.promote` method to ensure that the data swap
              is handled according to the specific sink's best practices
              (e.g., partition exchange, atomic renames, or transactional deletes).
        """
        start_ts = current_timestamp(naive=True).isoformat(sep=" ")

        try:
            loader = LoaderFactory.get_loader(self.config.type)
            load_ctx = LoadContext(
                target=self.config.destination,
                partition_on=self.config.partition_on,
                partition_value=self.config.partition_value,
                expected_count=self.write.write_count,
            )

            LOG.info(f"Promoting to {self.config.destination}")

            loader.promote(
                sink=self.sink,
                staging_id=self.write.staging_artifact,
                context=load_ctx,
            )

            payload = PublishPayload(
                final_path=self.config.destination,
                final_count=self.write.write_count,
                start_time=start_ts,
            )

            self.checkpoint(task, payload=payload)
            LOG.info(f"Publish complete: {self.config.destination}")
            return self._next_stage()

        except Exception as e:
            self.checkpoint(task, error=e)
            raise
