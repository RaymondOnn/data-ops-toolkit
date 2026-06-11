"""Write stage for loading transformed data to staging."""

from typing import TYPE_CHECKING, cast

from apps.ingestion.src.core.models.task.manifest import WritePayload
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
    from apps.ingestion.src.core.models.task.manifest import TransformPayload

LOG = logger


@stage(Stage.WRITE.value)
class WriteStage(ExecutionStage):
    """Stage for loading transformed data to staging area."""

    requires_disk_space: bool = False

    def pre_flight(self, task: "Task") -> None:
        """Verify sink connectivity and transform artifacts."""
        super().pre_flight(task)

        self.sink = ServiceFactory.get_sink(self.config.type, **self.config.service)

        # Verify transform metadata exists
        if task.manifest.transform is None:
            raise RollbackRequired(Stage.TRANSFORM.value, "Missing transform metadata")

        # Verify transform data exists
        transform_path = task.workspace.path / Stage.TRANSFORM.value
        if not transform_path.exists():
            raise RollbackRequired(Stage.TRANSFORM.value, "Missing transform data")

        # Verify parquet files exist if rows were expected
        transform = task.manifest.transform
        if transform.output_count and not any(transform_path.glob("*.parquet")):
            raise RollbackRequired(Stage.TRANSFORM.value, "Missing parquet files")

    def execute(self, task: "Task") -> str:
        """Load transformed data to staging."""
        start_ts = current_timestamp(naive=True).isoformat(sep=" ")
        extract = task.manifest.extract
        transform = cast("TransformPayload", task.manifest.transform)

        try:
            source_dir = (task.workspace.path / Stage.TRANSFORM.value).resolve()

            LOG.info(f"Loading to {self.config.destination}")

            loader = LoaderFactory.get_loader(self.config.type)
            load_ctx = LoadContext(
                target=self.config.destination,
                partition_by=self.config.partition_by,
                partition_value=self.config.partition_value,
                expected_count=transform.output_count,
            )

            audit = {
                "_partition": load_ctx.partition_value,
                "_run_id": task.run_id,
                "_source": (extract.resource if extract else None),
            }

            staging_id, rows = loader.stage(
                sink=self.sink,
                source_dir=source_dir,
                context=load_ctx,
                audit=audit,
            )

            payload = WritePayload(
                destination=self.config.destination,
                sink_type=self.config.type,
                staging_artifact=staging_id,
                write_count=rows,
                partition_by=self.config.partition_by or "",
                partition_value=self.config.partition_value or "",
                start_time=start_ts,
            )

            self.checkpoint(task, payload=payload)
            LOG.info(f"Load complete: {rows:_} rows staged to {staging_id}")
            return self._next_stage()

        except Exception as e:
            self.checkpoint(task, error=e)
            raise
