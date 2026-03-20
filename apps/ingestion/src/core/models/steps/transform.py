from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import msgspec
import polars as pl
import structlog
from src.core.models.job.manifest import TransformPayload
from src.core.models.steps import JobStep
from src.core.strategies.transform.base import TransformContext
from src.core.strategies.transform.factory import TransformFactory

from libs.file.formats.parquet import ParquetHandler

if TYPE_CHECKING:
    from src.core.models.job import Job


LOG = structlog.getLogger(__name__)
APP_TRANSFORM_OUTPUT_EXT = "parquet"


class TransformStep(JobStep):
    manifest: TransformPayload

    @property
    def name(self) -> str:
        return "transform"

    def execute(self, job: "Job") -> str:
        """
        Decision: Use LazyFrame Streaming for 50M rows.
        By reading from the 'active/extract' symlink, we ensure we are
        always processing the latest sanitized data without needing
        to know the specific physical timestamped folder.
        """
        start_ts = datetime.now(UTC).isoformat()
        try:
            with ParquetHandler() as handler:
                ctx = TransformContext(
                    options=job.context.transform.params,
                    source_dir=(job.folder / "extract" / "part_*.parquet").resolve(),
                    destination_dir=job.folder / "transform",
                    output_format=APP_TRANSFORM_OUTPUT_EXT,
                    type=job.context.transform.transform_type,
                )
                # 1. Initialize the LazyFrame (Logical Plan)
                # Decision: Use the 'active' symlink path.
                # Polars scans the metadata of all part_*.parquet files instantly.
                extract_path = (job.folder / "extract" / "part_*.parquet").resolve()
                lf = handler.to_df(extract_path)

                # 2. Apply Business Logic (Transformers)
                # These add to the 'Plan' but do not execute yet.
                # Decision: Use the factory to apply bitmasking and custom logic.
                transformer = TransformFactory.get_transformer(
                    job.context.transform.transform_type,
                    dataset_id=job.context.dataset_id,
                    job_id=job.id,
                )
                tr_lf = transformer.apply(lf, ctx)

                # 3. Stream to Physical Storage
                # Decision: Use sink_parquet via our handler's logic.
                # This triggers the Polars Rust engine to stream chunks.
                data_store = (
                    job.exec_ctx.workspace_dir
                    / "data"
                    / self.name
                    / f"{job.id}_{int(datetime.now(UTC).timestamp())}"
                )
                data_store.mkdir(parents=True, exist_ok=True)

                # 4. Decision: Use a partitioned sink.
                # This creates part-0.parquet, part-1.parquet, etc., in the data_store folder.
                # This is much safer for 2GB RAM as it flushes buffers more frequently.
                handler.from_df(tr_lf, str(data_store))

            # 5. DECISION: Get accurate stats after the stream is closed
            # scan_parquet + select(len) on the OUTPUT directory reads only the
            # file footers. This is near-instant even for 50M rows.
            stats = (
                pl.scan_parquet(str(data_store / "*.parquet"))
                .select(
                    count=pl.len(),
                    schema=pl.lit(str(tr_lf.schema)),
                )
                .collect()  # This is safe because it's only 1 row of metadata
            )

            # Polars' InProcessQuery object support
            if hasattr(stats, "fetch_blocking"):
                stats = cast("Any", stats).fetch_blocking()

            payload = TransformPayload(
                logic_version=getattr(transformer, "version", "1.0.0"),
                transform_type=job.context.transform.transform_type,
                artifact_folder=str(data_store),
                output_row_count=int(stats["count"][0]),
                schema_validation_pass=True,
                refined_schema={k: str(v) for k, v in tr_lf.schema.items()},
                start_timestamp_utc=start_ts,
            )

            # 4. Finalize & Flip the Link
            # Decision: Create active/{job_id}/transform -> ../../data/transform/{dir}
            # This makes the transformed data available for the WriteStep.
            self.finalize(job, results=msgspec.to_builtins(payload))
            return str(self._transit(job))

        except Exception as e:
            self.finalize(job=job, exception=e)
            raise
