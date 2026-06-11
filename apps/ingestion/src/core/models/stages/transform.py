"""Transform stage for data processing."""

from apps.ingestion.src.core.models.task import Task
from apps.ingestion.src.core.models.task.manifest import TransformPayload
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
class TransformStage(ExecutionStage):
    """Stage for transforming extracted data."""

    def pre_flight(self, task: Task) -> None:
        """Verify extract artifacts exist."""
        super().pre_flight(task)

        extract = task.manifest.extract
        extract_path = task.workspace.path / Stage.EXTRACT.value

        # Check conditions
        checks = [
            (extract is not None, "Extraction metadata missing"),
            (extract_path.exists(), "Extraction data marker missing"),
            (extract is None or extract.file_count > 0, "No data files found"),
        ]

        if extract and extract.file_count > 0:
            has_data = any(f.stat().st_size > 0 for f in extract_path.glob("*.parquet"))
            checks.append((has_data, "Physical artifacts missing or empty"))

        for ok, msg in checks:
            if not ok:
                raise RollbackRequired(Stage.EXTRACT.value, msg)

        # Validate transformer exists
        try:
            TransformFactory.get(self.config.type)
        except Exception:
            LOG.exception("Invalid transformer configuration")
            raise

        if not self.config.type:
            raise ValueError("Transform type not defined")

    def execute(self, task: Task) -> str:
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
