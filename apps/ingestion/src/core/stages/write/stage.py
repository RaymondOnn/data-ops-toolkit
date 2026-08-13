"""Write stage for loading transformed data to staging."""

from typing import TYPE_CHECKING

from libs.utils.dates import current_timestamp
from loguru import logger

from src.core.stages.contracts.stage import ExecutionStage, ExecutionStageRegistry
from src.core.stages.enums import Stage
from src.services.factory import ServiceFactory
from src.utils.exceptions import RollbackRequired

from .config import LoadConfig
from .enums import WritePayload
from .execution.load import LoadContext, LoaderFactory

if TYPE_CHECKING:
    from src.core.models.task import Task
    from src.core.models.task.manifest import TransformPayload

LOG = logger


@ExecutionStageRegistry.register(Stage.WRITE.value)
class WriteStage(ExecutionStage[LoadConfig]):
    """Stage for loading transformed data to staging area."""

    requires_disk_space: bool = False
    config_attribute = "write"
    transform: "TransformPayload"

    def pre_flight(self, task: "Task") -> None:
        """Verify sink connectivity and transform artifacts."""
        super().pre_flight(task)

        self.sink = ServiceFactory.get_sink(**self.config.connection)

        # Verify transform metadata exists
        if task.manifest.transform is None:
            raise RollbackRequired(Stage.TRANSFORM.value, "Missing transform metadata")

        self.transform = task.manifest.transform

        # Verify transform data exists
        transform_path = task.workspace.path / Stage.TRANSFORM.value
        if not transform_path.exists():
            raise RollbackRequired(Stage.TRANSFORM.value, "Missing transform data")

        # Verify parquet files exist if rows were expected
        if self.transform.output_count and not any(transform_path.glob("*.parquet")):
            raise RollbackRequired(Stage.TRANSFORM.value, "Missing parquet files")

    def _execute(self, task: "Task") -> str:
        """Load transformed data to staging."""
        start_ts = current_timestamp(naive=True).isoformat(sep=" ")
        extract = task.manifest.extract

        try:
            source_dir = (task.workspace.path / Stage.TRANSFORM.value).resolve()

            LOG.info(f"Loading to {self.config.destination}")

            loader = LoaderFactory.get_loader(self.config.type)
            load_ctx = LoadContext(
                target=self.config.destination,
                partition_on=task.context.partition_on,
                partition_value=task.context.partition_date,
                expected_count=self.transform.output_count,
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
                step_id=self.step_id,
                destination=self.config.destination,
                sink_type=self.config.type,
                staging_artifact=staging_id,
                write_count=rows,
                partition_on=self.config.partition_on or "",
                partition_value=self.config.partition_value or "",
                start_time=start_ts,
            )

            self.checkpoint(task, payload=payload)
            LOG.info(f"Load complete: {rows:_} rows staged to {staging_id}")
            return self._next_step(task)

        except Exception as e:
            self.checkpoint(task, error=e)
            raise
