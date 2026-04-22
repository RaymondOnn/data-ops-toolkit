from datetime import datetime
from typing import TYPE_CHECKING, Any

import msgspec
import polars as pl
import ray
from apps.ingestion.src.core.models.task.manifest import TransformPayload
from apps.ingestion.src.core.strategies.transform import (
    TransformContext,
    TransformFactory,
)
from loguru import logger

from .base import ExecutionStage
from .enums import StageName

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task


LOG = logger
APP_TRANSFORM_OUTPUT_EXT = "parquet"


class TransformStage(ExecutionStage):
    name = StageName.TRANSFORM.label
    manifest: TransformPayload

    def execute(self, task: "Task") -> str:
        """
        Decision: Use LazyFrame Streaming for 50M rows.
        By reading from the 'active/extract' symlink, we ensure we are
        always processing the latest sanitized data without needing
        to know the specific physical timestamped folder.
        """
        start_ts = datetime.now().astimezone().isoformat()
        LOG.info(
            "Starting transformation",
            stage=self.name,
            type=task.context.transform.transform_type,
        )
        try:
            # 1. Guard: Skip transformation if no files were extracted
            extract_payload = task.manifest.extract
            if not extract_payload or extract_payload.file_count == 0:
                LOG.info(
                    "No data extracted in previous stage. Skipping transformation.",
                    stage=self.name,
                )

                payload = TransformPayload(
                    logic_version="1.0.0",
                    transform_type=task.context.transform.transform_type,
                    artifact_folder="",
                    output_row_count=0,
                    schema_validation_pass=True,
                    refined_schema={},
                    start_timestamp_utc=start_ts,
                )

                self.finalize(task, results=msgspec.to_builtins(payload))
                return str(self._transit(task))

            # 1. Setup Context and Data Store
            extract_path = (task.folder / "extract").resolve()
            data_store = (
                task.exec_ctx.workspace_dir
                / "data"
                / self.name
                / f"{task.job_id}_{int(datetime.now().astimezone().timestamp())}"
            )
            data_store.mkdir(parents=True, exist_ok=True)

            ctx = TransformContext(
                options=task.context.transform.transform_params,
                source_dir=(task.folder / "extract" / "part_*.parquet").resolve(),
                destination_dir=task.folder / "transform",
                output_format=APP_TRANSFORM_OUTPUT_EXT,
                type=task.context.transform.transform_type,
            )

            # 2. Parallel Transformation via Ray Data
            # This reads all part_*.parquet files from the extract stage
            # into a distributed dataset
            ds = ray.data.read_parquet(str(extract_path))

            # 3. Define the Distributed Task
            # We capture the transformer type and params to recreate it on the workers
            transform_type = task.context.transform.transform_type
            dataset_id = task.context.dataset_id
            job_id = task.job_id

            def transform_batch(batch: Any) -> Any:
                # Re-initialize the transformer on the worker node
                # Ray provides pyarrow.Table when batch_format is "pyarrow"
                df = pl.from_arrow(batch)
                worker_transformer = TransformFactory.get_transformer(
                    transform_type,
                    dataset_id=dataset_id,
                    job_id=job_id,
                )
                # Apply transformation logic to this specific chunk
                # Ray 2.5+ requires a supported batch format (PyArrow, Pandas, etc.)
                # Converting to Arrow is zero-copy and satisfies the requirement.
                processed_df = worker_transformer.apply(df.lazy(), ctx).collect()
                return processed_df.to_arrow()

            # 4. Execute the Map and Write
            # map_batches handles the parallelism;
            # write_parquet produces multiple files automatically
            transformed_ds = ds.map_batches(transform_batch, batch_format="pyarrow")

            # Ray will write one file per task/partition
            # (e.g., part_000.parquet, part_001.parquet)
            transformed_ds.write_parquet(str(data_store))

            # Re-initialize a local transformer just for metadata/versioning info
            transformer = TransformFactory.get_transformer(
                transform_type, dataset_id=dataset_id, job_id=job_id
            )

            # 5. DECISION: Get accurate stats after the stream is closed
            # We scan the generated output to get the final row count and schema
            output_glob = str(data_store / "*.parquet")
            stats = pl.scan_parquet(output_glob).select(count=pl.len()).collect()

            output_rows = int(stats["count"][0])

            # 6. Extract final schema info from the generated artifacts
            sample_file = next(data_store.glob("*.parquet"))
            final_schema_dict = pl.read_parquet_schema(sample_file)

            payload = TransformPayload(
                logic_version=getattr(transformer, "version", "1.0.0"),
                transform_type=task.context.transform.transform_type,
                artifact_folder=str(data_store),
                output_row_count=output_rows or 0,
                schema_validation_pass=True,
                refined_schema={k: str(v) for k, v in final_schema_dict.items()},
                start_timestamp_utc=start_ts,
            )

            LOG.info(
                "Transformation complete",
                stage=self.name,
                output_rows=output_rows,
                output_folder=str(data_store.name),
            )

            # 4. Finalize & Flip the Link
            # Decision: Create active/{job_id}/transform -> ../../data/transform/{dir}
            # This makes the transformed data available for the WriteStep.
            self.finalize(
                task, data_folder=data_store, results=msgspec.to_builtins(payload)
            )
            return str(self._transit(task))

        except Exception as e:
            self.finalize(task=task, exception=e)
            raise
