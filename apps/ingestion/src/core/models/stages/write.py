from typing import TYPE_CHECKING, cast

import msgspec
from apps.ingestion.src.core.models.task.manifest import (
    TransformPayload,
    WritePayload,
)
from apps.ingestion.src.core.strategies.load.load import LoadContext, Loader
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.exceptions import RewindTask
from libs.utils.dates import get_current_timestamp
from loguru import logger

from .base import ExecutionStage
from .enums import StageName

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task


LOG = logger


class WriteStage(ExecutionStage):
    name = StageName.WRITE.label
    manifest: WritePayload

    def pre_flight(self, task: "Task") -> None:
        """Verify sink connectivity from the execution node."""
        super().pre_flight(task)

        # Factory initialization already validates basic params and Secret resolution
        self.service = ServiceFactory.get_sink(
            task.context.load.sink_type, **task.context.load.sink_config
        )

        # 1. Gate: Transform Metadata must exist
        transform_meta = task.manifest.transform
        if transform_meta is None:
            raise RewindTask(
                StageName.TRANSFORM.label, "Transformation metadata missing."
            )

        # 2. Gate: Transform Marker/Folder must exist
        transform_path = task.workspace.run_path / StageName.TRANSFORM.label
        if not transform_path.exists():
            raise RewindTask(
                StageName.TRANSFORM.label, "Transformation data marker missing."
            )

        # 3. Gate: Physical artifact verification
        # If the manifest indicates rows were processed, they must be present on disk
        if transform_meta.output_row_count > 0 and not any(
            transform_path.glob("*.parquet")
        ):
            raise RewindTask(
                StageName.TRANSFORM.label, "Transformed physical artifacts missing."
            )

    def execute(self, task: "Task") -> str:
        start_ts = get_current_timestamp(strip_tz=True).isoformat(sep=" ")
        task_ctx = task.context
        extract_meta = task.manifest.extract
        transform_meta = task.manifest.transform

        # Note: transform_meta is guaranteed by pre_flight at this point
        transform_meta = cast("TransformPayload", transform_meta)

        try:
            # 1. Resolve logical input (The partitioned parquet files)
            source_dir = (task.workspace.run_path / StageName.TRANSFORM.label).resolve()

            LOG.info(
                "Starting load into {target}",
                stage=self.name,
                sink_type=task_ctx.load.sink_type,
                target=task_ctx.load.sink_identifier,
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 3. Create Context
            context = LoadContext(
                sink_identifier=task_ctx.load.sink_identifier,
                partition_col=task_ctx.load.partition_col,
                partition_value=task_ctx.load.partition_value,
                expected_count=transform_meta.output_row_count or 0,
            )

            # 2. PHASE 1: LOAD TO STAGING
            audit_values = {
                "_partition": task_ctx.load.partition_value,
                "_run_id": task.run_id,
                "_source": (extract_meta.source_identifier if extract_meta else None)
                or task_ctx.extract.source_identifier,
            }
            staging_artifact, rows_loaded = loader.load(
                service=self.service,
                source_dir=source_dir,
                load_ctx=context,
                audit_values=audit_values,
            )

            # 3. Finalize Manifest
            payload = WritePayload(
                sink_identifier=task_ctx.load.sink_identifier,
                sink_type=task_ctx.load.sink_type,
                staging_artifact=staging_artifact,
                rows_inserted=int(rows_loaded),
                partition_col=task_ctx.load.partition_col or "",
                partition_value=task_ctx.load.partition_value or "",
                start_timestamp=start_ts,
            )

            self.finalize(task, results=msgspec.to_builtins(payload))
            LOG.info(
                "Load complete",
                stage=self.name,
                rows=int(rows_loaded),
                staging_artifact=staging_artifact,
            )
            return str(self._transit(task))

        except Exception as exc:
            self.finalize(task, exception=exc)
            raise exc
