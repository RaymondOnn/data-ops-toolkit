from abc import abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import Any

import polars as pl
import ray
import structlog
from src.core.strategies.extract import Reader, ReaderContext, ReaderFactory
from src.services.base import Service
from src.services.database import DatabaseService

LOG = structlog.getLogger(__name__)


class DataReader(Reader):
    """
    Base Strategy class.
    Subclasses implement _get_data_generator to handle source-specific logic.
    """

    def fetch(
        self, service: Service, context: ReaderContext, target_folder: Path
    ) -> list[dict[str, Any]]:
        """Entry point for ExtractStep."""
        df_generator = self._get_ray_generator(service, context)
        return self.to_parquet(df_generator, target_folder)

    def _get_ray_generator(
        self,
        service: Any,
        context: ReaderContext,
    ) -> Generator[pl.DataFrame, None, None]:
        # 1. Slice the work into parts (e.g., ORA_HASH queries)
        work_units = self.get_work_units(service, context)

        # 2. Package the metadata for the workers.
        # We don't send the 'service' object; we send the 'config' to recreate it.
        task_payloads = [
            {
                "unit": unit,
                "context": context,
                "config": service.config,  # Connection params
                "account_id": service.account_id,
            }
            for unit in work_units
        ]

        # 3. Create the distributed Ray Dataset
        ds = ray.data.from_items(task_payloads)

        # 3. Define the extraction task (Runs in parallel on Ray Workers)
        def fetch_task(payload: dict[str, Any]) -> pl.DataFrame:
            """This function runs on the Ray Worker (K8S Pod)."""
            from src.services.factory import ServiceFactory
            from src.core.schema import apply_schema_contract

            context = payload["context"]
            service = ServiceFactory.get_service(
                context.source_type, **payload["config"]
            )

            # 1. Extraction
            # Support both DB query string and File list dict
            unit = payload["unit"]
            if isinstance(unit, dict) and "files" in unit:
                unit = unit["files"]

            result = service.fetch_df(unit)

            # Convert LazyFrame to DataFrame for Ray compatibility
            df = result.collect() if isinstance(result, pl.LazyFrame) else result

            # 2. Guarding (Function Call)
            return apply_schema_contract(df, context)

        # 4. Map the task across the cluster
        # .iter_batches() makes this a generator!
        ray_dataset = ds.map(fetch_task)

        # 5. Yield blocks back to IngestionStream
        # Ray handles backpressure here: it only fetches the next block
        # when IngestionStream is ready for it.
        yield from ray_dataset.iter_batches(batch_format="polars")

    def to_parquet(
        self, generator: Generator[pl.DataFrame, None, None], destination: Path
    ) -> list[dict[str, Any]]:
        """
        Consumes the stream and saves each chunk as a unique parquet file.
        Returns metadata required for the FileInfo structs.
        """
        destination.mkdir(parents=True, exist_ok=True)
        metadata_list = []

        for i, df in enumerate(generator):
            if df.is_empty:
                continue

            file_path = destination / f"part_{i:04d}.parquet"

            # Write with snappy compression for a good balance of speed/size
            df.write_parquet(file_path, compression="snappy")

            # Capture metadata for the ExtractStep to process
            metadata_list.append(
                {"path": file_path, "rows": len(df), "schema": df.schema}
            )

        return metadata_list

    @abstractmethod
    def get_work_units(self, client: Any, context: ReaderContext) -> list[Any]:
        pass


@ReaderFactory.register("flat_file")
class FileDataReader(DataReader):
    def get_work_units(self, client: Any, context: ReaderContext) -> list[Any]:
        if not context.source_path:
            raise ValueError("source_path is required for FileDataReader")

        return client.get_work_units(context.source_path, context.num_partitions)


@ReaderFactory.register("database")
class DBDataReader(DataReader):
    def __init__(self) -> None:
        super().__init__()

    def get_work_units(
        self, client: DatabaseService, context: ReaderContext
    ) -> list[Any]:
        # Uses ORA_HASH for Oracle or ctid for Postgres
        # to generate N unique queries for the 50M rows
        if not context.target_table:
            raise ValueError("target_table is required for DBDataReader")

        filter_sql = context.options.get("filter_sql")
        units = client.get_work_units(
            context.target_table, context.num_partitions, filter_sql
        )
        return [str(unit) for unit in units]
