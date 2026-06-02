from abc import abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast

import msgspec
import polars as pl
import ray
from apps.ingestion.src.services.base import Source
from loguru import logger

from .base import Reader, ReaderContext
from .factory import ReaderFactory

LOG = logger

DEFAULT_COMPRESSION = "snappy"
BATCH_FORMAT_ARROW = "pyarrow"
PART_FILENAME_PATTERN = "part_{i:04d}.parquet"


class DataReader(Reader):
    """Base strategy for distributed data acquisition via Ray.

    Decision: Worker Isolation.
    By serializing the configuration and recreating service handles on
    Ray workers, we ensure that a failure in one worker (OOM) does not
    leak memory or state back to the Orchestrator.
    """

    def __init__(self) -> None:
        # Internal registry to track original source artifacts for the manifest
        # audit trail.
        self.source_files: list[str] = []

    def fetch(
        self, service: Source, context: ReaderContext, target_folder: Path
    ) -> Generator[dict[str, Any], None, None]:
        """Emits metadata for each file artifact as it is persisted.

        Decision: Continuous Checkpointing.
        By yielding metadata incrementally, the Stage can update the
        manifest in real-time, providing observability into long-running jobs.
        """
        df_generator = self._get_ray_generator(service, context)
        yield from self.to_parquet(df_generator, target_folder)

    def _get_ray_generator(
        self,
        service: Source,
        context: ReaderContext,
    ) -> Generator[pl.DataFrame, None, None]:
        """Initializes the Ray dataset and yields batches back to the caller.

        Decision: Backpressure Management.
        Ray iter_batches handles flow control; data is only pulled from
        workers as the local disk-writer (to_parquet) is ready to consume it.
        """
        work_units = self.get_work_units(service, context)

        # If units contain file lists (Standard for StorageSource),
        # capture them for the manifest audit trail.
        if work_units:
            # Check the first unit to see if it follows the file-based pattern
            first_unit = next(iter(work_units))
            if isinstance(first_unit, dict) and "files" in first_unit:
                for unit in work_units:
                    self.source_files.extend(unit.get("files", []))

                # Optimization: Don't spawn Ray tasks for empty file lists
                work_units = [u for u in work_units if u.get("files")]

        LOG.info(
            "Slicing extraction into work units",
            count=len(work_units),
            source=context.source_identifier,
        )

        # 2. Package the metadata for the workers.
        # We don't send the 'service' object; we send the 'config' to recreate it.
        task_payloads = [
            {
                "unit": unit,
                "context": msgspec.json.encode(context),
                "config": msgspec.json.encode(service.config),  # Connection params
            }
            for unit in work_units
        ]

        # 3. Create the distributed Ray Dataset
        ds = ray.data.from_items(task_payloads)

        # 3. Define the extraction task (Runs in parallel on Ray Workers)
        def fetch_task(batch: dict[str, list[Any]]) -> Any:
            """This function runs on the Ray Worker (K8S Pod)."""
            from pathlib import Path

            import msgspec
            import polars as pl
            from apps.ingestion.src.core.schema import apply_schema_contract
            from apps.ingestion.src.core.strategies.extract.base import ReaderContext
            from apps.ingestion.src.services.factory import ServiceFactory
            from apps.ingestion.src.services.registry import ServiceRegistry
            from loguru import logger as worker_logger

            # map_batches receives a batch (dict of lists). With batch_size=1,
            # we extract our single task payload from the first index.
            context = msgspec.json.decode(batch["context"][0], type=ReaderContext)
            config = msgspec.json.decode(batch["config"][0], type=dict)
            unit = batch["unit"][0]

            # Initialize the ServiceRegistry for this Ray worker process
            if context.workspace_dir:
                ServiceRegistry.configure(Path(context.workspace_dir))

            service = ServiceFactory.get_source(context.source_type, **config)

            worker_logger.info("Ray worker starting extraction task", unit=unit)

            # 1. Extraction
            # Support both DB query string and File list dict
            if isinstance(unit, dict) and "files" in unit:
                unit = unit["files"]

            result = service.fetch_data(unit)

            # Convert LazyFrame to DataFrame for Ray compatibility
            df = result.collect() if isinstance(result, pl.LazyFrame) else result

            row_count = df.height if isinstance(df, pl.DataFrame) else len(df)

            worker_logger.info(
                "Ray worker extracted {rows} rows from {unit}",
                rows=row_count,
                unit=unit,
            )

            # 2. Guarding (Function Call)
            # Ray 2.5+ requires a supported batch format (PyArrow, Pandas, etc.)
            # Converting to Arrow is zero-copy and satisfies the requirement.
            # We explicitly cast to pl.DataFrame to resolve the InProcessQuery union
            # conflict created by Ray's static type stubs.
            final_df = cast("pl.DataFrame", df)
            processed_df = apply_schema_contract(final_df, context)
            return processed_df.to_arrow()

        # 4. Map the task across the cluster
        # Use map_batches instead of map because fetch_task returns a Polars
        # DataFrame (a block of rows). Ray 2.5+ requires map_batches when
        # expanding a single task into a tabular result.
        ray_dataset = ds.map_batches(fetch_task, batch_size=1)

        # 5. Yield blocks back to IngestionStream
        # Ray handles backpressure here: it only fetches the next block
        # when IngestionStream is ready for it.

        # STRATEGY:
        # 1. If batch_size is in options, use it (Targeted Row Count).
        # 2. Otherwise, use None (Work-Unit based, one file per Ray partition).
        target_batch_size = context.options.get("batch_size")

        for batch in ray_dataset.iter_batches(
            batch_format=BATCH_FORMAT_ARROW, batch_size=target_batch_size
        ):
            res = pl.from_arrow(batch)
            # Ensure we yield a DataFrame to satisfy the Generator type hint
            yield res if isinstance(res, pl.DataFrame) else res.to_frame()

    def to_parquet(
        self, generator: Generator[pl.DataFrame, None, None], destination: Path
    ) -> Generator[dict[str, Any], None, None]:
        """Consumes the stream and saves each chunk as a unique parquet file.

        Args:
            generator: A stream of Polars DataFrames from Ray workers.
            destination: Physical directory to save Parquet artifacts.

        Decision: Zero-Copy Format.
        We write to Parquet immediately after extraction to ensure subsequent
        stages (Transform/Write) benefit from columnar compression and
        predicate pushdown.
        """
        destination.mkdir(parents=True, exist_ok=True)

        for i, df in enumerate(generator):
            if df.is_empty():
                LOG.debug("Skipping empty DataFrame chunk", chunk_index=i)
                continue

            # Ensure the type checker knows this is a Sized Polars object
            # and use .height for unambiguous row counting
            row_count = df.height

            file_path = destination / PART_FILENAME_PATTERN.format(i=i)

            # Write with snappy compression for a good balance of speed/size
            df.write_parquet(file_path, compression=DEFAULT_COMPRESSION)
            LOG.info("Exported parquet chunk", path=str(file_path), rows=row_count)

            # Yield metadata back to the Stage for checkpointing
            yield {"path": file_path, "rows": row_count, "schema": df.schema}

    @abstractmethod
    def get_work_units(self, service: Source, context: ReaderContext) -> set[Any]:
        """Calculates parallel work units for distributed execution.

        Args:
            service: The data source service instance.
            context: The reader context containing ingestion parameters.

        Returns:
            set[Any]: A set of work unit definitions.
        """
        pass


@ReaderFactory.register("flat_file")
class FileDataReader(DataReader):
    def _resolve_target_params(
        self, target: str, file_pattern: str | dict[str, str] | None
    ) -> tuple[str, str | None]:
        """Normalizes structured path configurations into physical parameters.

        Args:
            target: The primary path or identifier.
            file_pattern: Optional glob string or structured dict.

        Returns:
            tuple: (physical_target_path, optional_archive_container_path).

        Decision: Strategy-Side Normalization.
        The interpretation of `{archive, glob}` is a business rule of the
        ingestion app. Moving this to the Strategy layer keeps our
        generic Storage Services focused strictly on filesystem operations.
        """
        archive_path = None
        target_path = target

        if isinstance(file_pattern, dict):
            archive_path = file_pattern.get("archive")
            target_path = file_pattern.get("glob", target_path)
        elif file_pattern and not target:
            target_path = file_pattern

        return target_path, archive_path

    def get_work_units(self, service: Source, context: ReaderContext) -> set[Any]:
        """Resolves parallel work units by normalizing ingestion parameters.

        Decision: Pre-flight Resolution.
        We resolve the physical path and archive container here. This ensures
        that the Service layer (StorageSource) receives explicit parameters
        rather than having to guess the structure of 'options'.
        """
        target_path, archive_path = self._resolve_target_params(
            target=context.source_identifier or "",
            file_pattern=context.options.get("file_pattern"),
        )

        if not target_path and not archive_path:
            raise ValueError("A source identifier or file_pattern is required.")

        return service.parallelize(
            target=target_path,
            num_workers=context.num_workers,
            filter_condition=context.options.get("filter_condition"),
            archive_path=archive_path,
        )


@ReaderFactory.register("database")
class DBDataReader(DataReader):
    def get_work_units(self, service: Source, context: ReaderContext) -> set[Any]:
        """Resolves the database work units via the service layer.

        Args:
            service: The DatabaseSource instance.
            context: The reader context containing target table and workers.

        Decision: Delegated Optimization.
        The optimization logic based on cell-count is implemented in the
        Service Layer. This allows the Service to decide between standard
        partitioning or intra-table slicing based on the table's width.
        """
        if not context.source_identifier:
            raise ValueError("target_table is required for DBDataReader")

        condition = context.options.get("filter_condition") or context.options.get(
            "filter_sql"
        )
        # Passing kwargs allows the Service to access 'schema_items'
        # for cell-count calculations if needed.
        units = service.parallelize(
            context.source_identifier,
            context.num_workers,
            condition,
            schema_items=context.schema_items,
        )
        return {str(unit) for unit in units}
