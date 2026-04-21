from datetime import datetime
from typing import TYPE_CHECKING

import msgspec
from apps.ingestion.src.core.models.job.manifest import WritePayload
from apps.ingestion.src.core.strategies.load.load import LoadContext, Loader
from apps.ingestion.src.services.factory import ServiceFactory
from loguru import logger

from .base import ExecutionStage
from .enums import StageName

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.job import Task


LOG = logger


class WriteStage(ExecutionStage):
    name = StageName.WRITE.label
    manifest: WritePayload

    def pre_flight(self, task: "Task") -> None:
        """Verify sink connectivity from the execution node."""
        # Factory initialization already validates basic params and Secret resolution
        self.service = ServiceFactory.get_sink(
            task.context.load.sink_type, **task.context.load.sink_config
        )

    def execute(self, task: "Task") -> str:
        start_ts = datetime.now().astimezone().isoformat()
        task_ctx = task.context
        transform_meta = task.manifest.transform
        if not transform_meta:
            raise ValueError(
                "Transform metadata is required in the manifest for the WRITE stage."
            )

        try:
            # 1. Resolve logical input (The partitioned parquet files)
            source_dir = (task.folder / "transform").resolve()

            # Verify source_dir actually contains files before proceeding
            if not any(source_dir.glob("*.parquet")):
                raise FileNotFoundError(
                    f"No parquet files found in transformed data directory: {source_dir}"
                )

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
                "_source": task_ctx.extract.source_identifier,
            }
            print("Audit Values in WriteStage:", audit_values)
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
                start_timestamp_utc=start_ts,
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
