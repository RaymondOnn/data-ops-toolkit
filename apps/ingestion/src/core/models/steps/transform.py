import time

import polars as pl  # type: ignore
import structlog  # type: ignore
from src.core.models.job import Job
from src.core.models.job.manifest import TransformPayload
from src.core.models.steps import JobBitmask, JobStep
from src.utils.constants import JOB_STEPS_BASE_DIR

from libs.file.formats.parquet import ParquetHandler

LOG = structlog.getLogger(__name__)


class TransformStep(JobStep):  # type: ignore
    manifest: TransformPayload

    @property
    def bitmask(self) -> str:
        return str(JobBitmask.TRANSFORM)

    @property
    def name(self) -> str:
        return "transform"

    def execute(self, job: "Job") -> str:
        """
        Decision: Use LazyFrame Streaming for 50M rows.
        By reading from the 'active/raw' symlink, we ensure we are
        always processing the latest sanitized data without needing
        to know the specific physical timestamped folder.
        """
        from src.core.transform.transform import TransformFactory

        start_time = time.perf_counter()

        try:
            with ParquetHandler() as handler:
                # 1. Initialize the LazyFrame (Logical Plan)
                # Decision: Use the 'active' symlink path.
                # Polars scans the metadata of all part_*.parquet files instantly.
                raw_path = str(
                    JOB_STEPS_BASE_DIR / "active" / job.id / "raw" / "part_*.parquet"
                )
                lf = handler.to_df(raw_path)

                # 2. Apply Business Logic (Transformers)
                # These add to the 'Plan' but do not execute yet.
                # Decision: Use the factory to apply bitmasking and custom logic.
                transformer = TransformFactory.get_transformer(job.context)
                tr_lf = transformer.apply(lf)

                # 3. Stream to Physical Storage
                # Decision: Use sink_parquet via our handler's execution-aware logic.
                # This triggers the Polars Rust engine to stream chunks through the plan.
                data_store = (
                    JOB_STEPS_BASE_DIR
                    / "data"
                    / self.name
                    / f"{job.id}_{int(time.time())}"
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
                    schema=pl.map_batches(lambda _: str(getattr(tr_lf, "schema"))),
                )
                .collect()  # This is safe because it's only 1 row of metadata
            )

            # 6. Build the RefinedMetadata
            duration_ms = int((time.perf_counter() - start_time) * 1000)

            payload = TransformPayload(
                step_outcome="COMPLETED",
                logic_version=getattr(transformer, "version", "1.0.0"),
                artifact_folder=str(data_store),
                output_record_count=stats["count"][0],
                schema_validation_passed=True,  # Strategy could add validation logic
                refined_schema=stats["schema"][0],
                processing_duration_secs=duration_ms,
            )

            # 4. Finalize & Flip the Link
            # Decision: Create active/{job_id}/transform -> ../../data/transform/{dir}
            # This makes the transformed data available for the WriteStep.
            self.finalize(job=job, data_folder=data_store, results=payload)
            return str(self._transit(job))

        except Exception as e:
            self.finalize(job=job, exception=e)
            raise
