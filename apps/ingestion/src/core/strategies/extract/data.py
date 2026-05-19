from abc import abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast

import msgspec
import polars as pl
import ray
from apps.ingestion.src.services.base import Source
from apps.ingestion.src.services.database import DatabaseSource
from loguru import logger

from .base import Reader, ReaderContext
from .factory import ReaderFactory

LOG = logger


class DataReader(Reader):
    """
    Base Strategy class.
    Subclasses implement _get_data_generator to handle source-specific logic.
    """

    def __init__(self) -> None:
        # Internal registry to track original source artifacts for the manifest
        # audit trail.
        self.source_files: list[str] = []

    def fetch(
        self, service: Source, context: ReaderContext, target_folder: Path
    ) -> Generator[dict[str, Any], None, None]:
        """Emits metadata for each file artifact as it is created."""
        df_generator = self._get_ray_generator(service, context)
        yield from self.to_parquet(df_generator, target_folder)

    def _get_ray_generator(
        self,
        service: Source,
        context: ReaderContext,
    ) -> Generator[pl.DataFrame, None, None]:
        # 1. Slice the work into parts (e.g., ORA_HASH queries)
        work_units = self.get_work_units(service, context)

        # If units contain file lists (Standard for StorageSource),
        # capture them for the manifest audit trail.
        if work_units:
            # Check the first unit to see if it follows the file-based pattern
            first_unit = next(iter(work_units))
            if isinstance(first_unit, dict) and "files" in first_unit:
                for unit in work_units:
                    self.source_files.extend(unit.get("files", []))

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
                "Ray worker completed extraction task",
                rows=row_count,
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
        for batch in ray_dataset.iter_batches(batch_format="pyarrow"):
            res = pl.from_arrow(batch)
            # Ensure we yield a DataFrame to satisfy the Generator type hint
            yield res if isinstance(res, pl.DataFrame) else res.to_frame()

    def to_parquet(
        self, generator: Generator[pl.DataFrame, None, None], destination: Path
    ) -> Generator[dict[str, Any], None, None]:
        """
        Consumes the stream and saves each chunk as a unique parquet file.
        Yields metadata immediately for real-time progress tracking.
        """
        destination.mkdir(parents=True, exist_ok=True)

        for i, df in enumerate(generator):
            if df.is_empty():
                LOG.debug("Skipping empty DataFrame chunk", chunk_index=i)
                continue

            # Ensure the type checker knows this is a Sized Polars object
            # and use .height for unambiguous row counting
            row_count = df.height

            file_path = destination / f"part_{i:04d}.parquet"

            # Write with snappy compression for a good balance of speed/size
            df.write_parquet(file_path, compression="snappy")
            LOG.info("Exported parquet chunk", path=str(file_path), rows=row_count)

            # Yield metadata back to the Stage for checkpointing
            yield {"path": file_path, "rows": row_count, "schema": df.schema}

    @abstractmethod
    def get_work_units(self, client: Any, context: ReaderContext) -> set[Any]:
        pass


@ReaderFactory.register("flat_file")
class FileDataReader(DataReader):
    def get_work_units(self, client: Any, context: ReaderContext) -> set[Any]:
        source_path = context.source_identifier or context.options.get(
            "file_pattern", ""
        )
        print("source_path", source_path)
        if not source_path:
            raise ValueError(
                "source_identifier or file_pattern is required for FileDataReader"
            )

        return client.get_work_units(source_path, context.num_workers)


@ReaderFactory.register("database")
class DBDataReader(DataReader):
    def __init__(self) -> None:
        super().__init__()

    def get_work_units(
        self, client: DatabaseSource, context: ReaderContext
    ) -> set[Any]:
        # Uses ORA_HASH for Oracle or ctid for Postgres
        # to generate N unique queries for the 50M rows
        if not context.source_identifier:
            raise ValueError("target_table is required for DBDataReader")

        filter_sql = context.options.get("filter_sql")
        units = client.get_work_units(
            context.source_identifier, context.num_workers, filter_sql
        )
        return {str(unit) for unit in units}
