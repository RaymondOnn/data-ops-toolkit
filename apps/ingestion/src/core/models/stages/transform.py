"""Transform stage for data processing."""

from apps.ingestion.src.core.contexts import TransformConfig
from apps.ingestion.src.core.models.task import Task
from apps.ingestion.src.core.models.task.manifest import (
    ExtractPayload,
    TransformPayload,
)
from apps.ingestion.src.core.strategies.transform import (
    TRANSFORMERS,
    TransformContext,
    TransformFactory,
)
from apps.ingestion.src.utils.exceptions import RollbackRequired
from libs.utils.dates import current_timestamp
from loguru import logger

from .base import ExecutionStage
from .enums import Stage
from .utils import stage

LOG = logger
OUTPUT_FORMAT = "parquet"


@stage(Stage.TRANSFORM.value)
class TransformStage(ExecutionStage[TransformConfig]):
    """Stage for transforming extracted data."""

    config_attribute = "transform"
    extract: "ExtractPayload"

    def pre_flight(self, task: Task) -> None:
        """Verify extract artifacts exist."""
        super().pre_flight(task)

        if not task.manifest.extract:
            raise RollbackRequired(Stage.EXTRACT.value, "Extraction metadata missing")

        self.extract = task.manifest.extract
        extract_path = task.workspace.path / Stage.EXTRACT.value
        if not extract_path.exists():
            raise RollbackRequired(
                Stage.EXTRACT.value, "Extraction data marker missing"
            )

        if not self.extract.file_count > 0:
            raise RollbackRequired(Stage.EXTRACT.value, "No data files found")

        if not any(f.stat().st_size > 0 for f in extract_path.glob("*.parquet")):
            raise RollbackRequired(
                Stage.EXTRACT.value, "Physical artifacts missing or empty"
            )

        # Validate transformer exists
        try:
            TransformFactory.get(self.config.type)
        except Exception:
            LOG.exception("Invalid transformer configuration")
            raise

    def _execute(self, task: Task) -> str:
        """Execute transformation using distributed executor."""
        start_time = current_timestamp(naive=True).isoformat(sep=" ")
        LOG.info(f"Starting transform: {self.config.type}")

        try:
            source_path = (task.workspace.path / Stage.EXTRACT.value).resolve()
            target_path = task.workspace.reset_data_dir(self.name)

            ctx = TransformContext(
                params=self.config.params,
                source=source_path,
                target=target_path,
                format=OUTPUT_FORMAT,
                logic=self.config.type,
                job_id=task.job_id,
                dataset_id=task.dataset_id,
            )

            # TODO: Add support for other executors
            # Execute distributed transformation
            transformer = TRANSFORMERS["data"]()
            row_count, schema = transformer.transform(context=ctx)

            payload = TransformPayload(
                transform_type=self.config.type,
                output_count=row_count,
                artifact_folder=str(target_path),
                schema_valid=True,
                output_schema=schema,
                start_time=start_time,
            )

            self.checkpoint(task, data_folder=target_path, payload=payload)
            LOG.info(f"Transform complete: {row_count:_} rows -> {target_path.name}")
            return self._next_stage()

        except Exception as e:
            self.checkpoint(task, error=e)
            raise
