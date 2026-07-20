from abc import abstractmethod
from collections.abc import Generator, Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any, Generic, TypeVar

import msgspec
import polars as pl
import ray
from apps.ingestion.src.services.base import Source
from apps.ingestion.src.services.database import DatabaseSource, SQLContext
from apps.ingestion.src.services.file import StorageSource
from loguru import logger

from .base import ExtractContext, Extractor
from .factory import ExtractorFactory

LOG = logger
PART_FILENAME = "part_{i:04d}.parquet"

T_Source = TypeVar("T_Source", bound=Source)


def flatten_df(df: pl.DataFrame, flatten_val: int) -> pl.DataFrame:
    """Recursively unnests Polars Struct columns up to the specified depth."""
    if flatten_val == -1:
        return df

    # OPTIONAL BUT SAFE: Parse string columns that contain stringified JSON
    for col in df.columns:
        if df.schema[col] == pl.String:
            # Check the first non-null row to see if it looks like a JSON object
            first_val = df[col].drop_nulls().first()
            if isinstance(first_val, str) and first_val.strip().startswith("{"):
                # Fallback if it's just a regular string that happens to look like JSON
                with suppress(Exception):
                    # Pass pl.Unknown to let Polars infer the struct schema dynamically
                    df = df.with_columns(pl.col(col).str.json_decode(dtype=pl.Unknown))

    # Normalize depth boundaries: True or 0 both mean infinite depth
    max_depth = float("inf") if flatten_val == 0 else int(flatten_val)
    current_depth = 0

    while current_depth < max_depth:
        # Find all columns that are currently Structs
        struct_cols = [
            col
            for col, dtype in zip(df.columns, df.dtypes, strict=False)
            if isinstance(dtype, pl.Struct)
        ]

        # If no struct columns remain, we are fully flat
        if not struct_cols:
            break

        # Unnest the struct columns.
        # Polars automatically expands them using 'parent_child' naming styles if desired,
        # or defaults to the inner field names. To ensure lineage preservation:
        for col in struct_cols:
            struct_field_names = df[col].struct.fields

            df = df.with_columns(
                [
                    pl.col(col).struct.field(name).alias(f"{col}_{name}")
                    for name in struct_field_names
                ]
            ).drop(col)

        current_depth += 1
    return df


def log_df_attributes(df) -> None:
    rows, cols = df.shape

    LOG.debug("--- Polars DataFrame Metadata ---")
    LOG.debug(f"Dimensions : {rows} rows, {cols} columns")
    LOG.debug(f"Columns    : {df.columns}")

    # df.schema returns a dictionary-like object mapping column names to data types
    # Using a single-line or pretty format for the schema dict
    LOG.debug(f"Schema     : {dict(df.schema)}")


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
        self, source: T_Source, context: ExtractContext, target_folder: Path
    ) -> Generator[dict[str, Any], None, None]:
        """Extract data from source and write to Parquet.

        Implementation of the generic Extractor interface for distributed
        processing via Ray.
        """
        yield from self._write_parquet(
            self._ray_generator(source, context), target_folder
        )

    def _ray_generator(
        self, source: T_Source, context: ExtractContext
    ) -> Generator[pl.DataFrame, None, None]:
        """Orchestrate the distributed extraction via Ray.

        Args:
            source: The source service.
            context: The extraction context.

        Yields:
            pl.DataFrame: Chunks of data as Polars DataFrames.
        """
        work_units = self._get_work_units(source, context)
        if not work_units:
            raise ValueError("No work units generated")

        self._capture_source_files(work_units)
        LOG.info(f"Slicing into {len(work_units)} units from {context.resource}")

        # Package for Ray
        payloads = [
            {
                "unit": unit,
                "context": msgspec.json.encode(context),
                "config": msgspec.json.encode(source.config),
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
            from apps.ingestion.src.core.strategies.extract.data import flatten_df
            from apps.ingestion.src.core.strategies.extract.schema import (
                apply_schema_contract,
            )
            from apps.ingestion.src.services.factory import ServiceFactory
            from loguru import logger as worker_log

            ctx = msgspec.json.decode(batch["context"][0], type=ExtractContext)
            # config = msgspec.json.decode(batch["config"][0], type=dict)
            unit = batch["unit"][0]

            if ctx.workspace:
                ServiceMonitor.setup(**ctx.monitor_params)

            svc = ServiceFactory.get_source(**ctx.src_connection)
            worker_log.info(f"Worker processing: {unit}")

            result = svc.pull(unit)
            df = result.collect() if isinstance(result, pl.LazyFrame) else result

            worker_log.info(f"Extracted {df.height:_} rows")
            df = flatten_df(df, ctx.flatten)
            processed = apply_schema_contract(df, ctx)
            log_df_attributes(processed)
            return processed.to_arrow()

        ray_dataset = ds.map_batches(worker_task, batch_size=1)
        batch_size = context.batch_size

        # 3. Enforce the Global Limit on the driver during the streaming phase
        global_limit = context.limit
        accumulated_rows = 0

        for batch in ray_dataset.iter_batches(
            batch_format="pyarrow", batch_size=batch_size
        ):
            # Check if limit was met in previous iterations
            if global_limit is not None and accumulated_rows >= global_limit:
                LOG.info("Global limit threshold met. Halting Ray generation.")
                break

            df = pl.from_arrow(batch)
            # from_arrow can return Series if only one column
            if not isinstance(df, pl.DataFrame):
                df = df.to_frame()

            chunk_rows = df.height
            if chunk_rows == 0:
                continue

            if (
                global_limit is not None
                # If this incoming batch pushes us over the global boundary, slice it
                and accumulated_rows + chunk_rows > global_limit
            ):
                allowed_rows = global_limit - accumulated_rows
                df = df.slice(0, allowed_rows)
                accumulated_rows += allowed_rows
                yield df
                break  # Stop fetching subsequent batches from Ray

            accumulated_rows += chunk_rows
            yield df

    def _capture_source_files(self, work_units: Sequence) -> None:
        """Record the source files being processed for audit.

        Args:
            work_units: The list of generated work units.
        """
        if work_units and isinstance(work_units[0], dict) and "files" in work_units[0]:
            for unit in work_units:
                self.source_files.extend(unit.get("files", []))

    @staticmethod
    def _write_parquet(
        gen: Generator[pl.DataFrame, None, None], dest: Path
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
    def _get_work_units(
        self, source: T_Source, context: ExtractContext
    ) -> Sequence[Any]:
        """Calculate work units for parallel processing.

        Args:
            source: The source service.
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
    ) -> Sequence[Any]:
        """Retrieve work units from a storage source.

        Args:
            source: The storage service.
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
            # archive kwargs
            cleanup=context.tmp_cleanup,
            temp_folder=context.task_folder,
            # data kwargs
            select=context.select,
            where=context.where,
            limit=context.limit,
            sql=context.sql,
            # file kwargs
            skip_blank_lines=context.skip_blank_lines,
            header=context.header,
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
        # pattern = context.params.get("file_pattern")
        glob = context.glob

        # If pattern is None and resource has wildcards, split it
        if not glob and target and ("*" in target or "?" in target):
            # This handles cases where resource might still contain wildcards
            path = Path(target)
            target = str(path.parent) if path.parent != path else "."
            glob = path.name

        if glob:
            glob = glob.format(partition_date=context.partition_date)

        LOG.debug(f"Resolved target: {target}, pattern: {glob}")
        return target, glob


@ExtractorFactory.register("database")
class DatabaseExtractor(DataExtractor[DatabaseSource]):
    """Extractor specialized for SQL databases."""

    def _get_work_units(
        self, source: DatabaseSource, context: ExtractContext
    ) -> Sequence[Any]:
        """Retrieve work units from a database source.

        Args:
            source: The database service.
            context: The extraction context.

        Returns:
            list[Any]: A list of query-based work units.
        """
        if not context.resource:
            raise ValueError("Resource required for DB extractor")

        # 1. Build the SQLContext from the general ExtractContext variables
        # Note: You can extract 'sql' out of context.params if it lives there
        sql_ctx = SQLContext(
            resource=context.resource,
            sql=context.sql,
            select=context.select,
            where=context.where,
            limit=context.limit,
        )
        units = source.parallelize(
            context.resource,
            context.num_workers,
            sql_context=sql_ctx,
        )
        return list(units)
