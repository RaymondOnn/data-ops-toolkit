from abc import abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import Any, Generic, TypeVar

import msgspec
import polars as pl
import ray
from apps.ingestion.src.services.base import Source
from apps.ingestion.src.services.database import DatabaseSource
from apps.ingestion.src.services.file import StorageSource
from loguru import logger

from .base import ExtractContext, Extractor
from .factory import ExtractorFactory

LOG = logger
PART_FILENAME = "part_{i:04d}.parquet"

T_Source = TypeVar("T_Source", bound=Source)


class DataExtractor(Extractor[T_Source], Generic[T_Source]):
    """Ray-based distributed extractor with streaming Parquet output.

    Instead of loading the entire dataset into memory, this extractor
    streams results to Parquet files on disk. This allows it to handle
    datasets much larger than available RAM on a single node.
    """

    def __init__(self):
        """Initialize the data extractor."""
        self.source_files: list[str] = []

    def extract(
        self, service: T_Source, context: ExtractContext, target_folder: Path
    ) -> Generator[dict[str, Any], None, None]:
        """Extract data from source and write to Parquet.

        Implementation of the generic Extractor interface for distributed
        processing via Ray.
        """
        yield from self._write_parquet(
            self._ray_generator(service, context), target_folder
        )

    def _ray_generator(
        self, service: T_Source, context: ExtractContext
    ) -> Generator[pl.DataFrame, None, None]:
        """Orchestrate the distributed extraction via Ray.

        Args:
            service: The source service.
            context: The extraction context.

        Yields:
            pl.DataFrame: Chunks of data as Polars DataFrames.
        """
        work_units = self._get_work_units(service, context)
        if not work_units:
            raise ValueError("No work units generated")

        self._capture_source_files(work_units)
        LOG.info(f"Slicing into {len(work_units)} units from {context.resource}")

        # Package for Ray
        payloads = [
            {
                "unit": unit,
                "context": msgspec.json.encode(context),
                "config": msgspec.json.encode(service.config),
            }
            for unit in work_units
        ]
        ds = ray.data.from_items(payloads)

        def worker_task(batch: dict[str, list[Any]]) -> Any:
            """Ray worker task to pull data for a single unit.

            Args:
                batch: A single-item batch containing the work unit and context.

            Returns:
                pyarrow.Table: The extracted and schema-aligned data.
            """

            import msgspec
            import polars as pl
            from apps.ingestion.src.core.monitor import ServiceMonitor
            from apps.ingestion.src.core.schema import apply_schema_contract
            from apps.ingestion.src.services.factory import ServiceFactory
            from loguru import logger as worker_log

            ctx = msgspec.json.decode(batch["context"][0], type=ExtractContext)
            config = msgspec.json.decode(batch["config"][0], type=dict)
            unit = batch["unit"][0]

            if ctx.workspace:
                ServiceMonitor.setup(**ctx.monitor_params)

            svc = ServiceFactory.get_source(ctx.kind, **config)
            worker_log.info(f"Worker processing: {unit}")

            result = svc.pull(unit)
            df = result.collect() if isinstance(result, pl.LazyFrame) else result

            worker_log.info(f"Extracted {df.height:_} rows")
            processed = apply_schema_contract(df, ctx)
            return processed.to_arrow()

        ray_dataset = ds.map_batches(worker_task, batch_size=1)
        batch_size = context.params.get("batch_size")

        for batch in ray_dataset.iter_batches(
            batch_format="pyarrow", batch_size=batch_size
        ):
            df = pl.from_arrow(batch)
            # from_arrow can return Series if only one column
            if not isinstance(df, pl.DataFrame):
                df = df.to_frame()
            yield df

    def _capture_source_files(self, work_units: list) -> None:
        """Record the source files being processed for audit.

        Args:
            work_units: The list of generated work units.
        """
        if work_units and isinstance(work_units[0], dict) and "files" in work_units[0]:
            for unit in work_units:
                self.source_files.extend(unit.get("files", []))

    def _write_parquet(
        self, gen: Generator[pl.DataFrame, None, None], dest: Path
    ) -> Generator[dict[str, Any], None, None]:
        """Serialize generator output to snappy-compressed Parquet files.

        Args:
            gen: The generator yielding DataFrames.
            dest: Target directory.

        Yields:
            dict[str, Any]: Manifest of the written file.
        """
        dest.mkdir(parents=True, exist_ok=True)
        for i, df in enumerate(gen):
            if df.is_empty():
                continue
            path = dest / PART_FILENAME.format(i=i)
            df.write_parquet(path, compression="snappy")
            LOG.info(f"Wrote {df.height:_} rows to {path}")
            yield {"path": path, "rows": df.height, "schema": df.schema}

    @abstractmethod
    def _get_work_units(self, source: T_Source, context: ExtractContext) -> list[Any]:
        """Calculate work units for parallel processing.

        Args:
            service: The source service.
            context: The extraction context.

        Returns:
            list[Any]: A list of work units.
        """
        pass


@ExtractorFactory.register("flat_file")
class FileExtractor(DataExtractor[StorageSource]):
    """Extractor specialized for filesystem-based storage."""

    def _get_work_units(
        self, source: StorageSource, context: ExtractContext
    ) -> list[Any]:
        """Retrieve work units from a storage source.

        Args:
            service: The storage service.
            context: The extraction context.

        Returns:
            list[Any]: A list of file-based work units.
        """
        target, pattern = self.resolve_file_params(context)
        LOG.debug(f"Resolved target: {target}, pattern: {pattern}")
        return source.parallelize(
            target=target,
            num_workers=context.num_workers,
            filter_condition=pattern,
            cleanup=context.params.get("cleanup", True),
            temp_folder=context.params.get("workspace"),
        )

    @staticmethod
    def resolve_file_params(context: ExtractContext) -> tuple[str, str | None]:
        """Resolve target path and pattern for file extraction.

        Note: This method is used at extraction time, not configuration time.
        It converts the already-resolved ExtractConfig into runtime parameters.
        """
        # Use resource as target (already resolved during config)
        target = context.resource

        # Get pattern from params (set during config resolution)
        pattern = context.params.get("file_pattern")

        # If pattern is None and resource has wildcards, split it
        if not pattern and target and ("*" in target or "?" in target):
            # This handles cases where resource might still contain wildcards
            path = Path(target)
            target = str(path.parent) if path.parent != path else "."
            pattern = path.name

        LOG.debug(f"Resolved target: {target}, pattern: {pattern}")
        return target, pattern


@ExtractorFactory.register("database")
class DatabaseExtractor(DataExtractor[DatabaseSource]):
    """Extractor specialized for SQL databases."""

    def _get_work_units(
        self, source: DatabaseSource, context: ExtractContext
    ) -> list[Any]:
        """Retrieve work units from a database source.

        Args:
            service: The database service.
            context: The extraction context.

        Returns:
            list[Any]: A list of query-based work units.
        """
        if not context.resource:
            raise ValueError("Resource required for DB extractor")
        condition = context.params.get("filter_condition")
        units = source.parallelize(
            context.resource,
            context.num_workers,
            condition,
            schema=context.schema,
        )
        return list(units)
