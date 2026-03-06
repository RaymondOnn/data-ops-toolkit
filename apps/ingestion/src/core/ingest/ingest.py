from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Generator, Optional, List, Dict

import msgspec

import ray
import polars as pl
import structlog

LOG = structlog.getLogger(__name__)

class ReaderContext(msgspec.Struct):
    """
    Type-safe container for all ingestion parameters.
    Serializable for Ray worker distribution.
    """
    source_type: str
    target_table: Optional[str] = None  # Used by DatabaseIngest
    source_path: Optional[str] = None   # Used by FileIngest
    parallelism: int = 10
    # For any source-specific extras (e.g., API keys, custom filters)
    options: Dict[str, Any] = {}


class Reader(ABC):
    @abstractmethod
    def fetch(
        self, 
        client: Any, 
        context: ReaderContext, 
        target_folder: Path
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("Subclasses must implement this method")

class FileIngest(Reader):
    def get_units(self, client: Any, context: ReaderContext) -> List[str]:
        # context.source_path provides the directory or bucket to scan
        return client.list_files(context.source_path)


class DataReader(Reader):
    """
    Base Strategy class. 
    Subclasses implement _get_data_generator to handle source-specific logic.
    """
    
    def fetch(
        self, 
        service: Any, 
        context: ReaderContext, 
        target_folder: Path
    ) -> List[Dict[str, Any]]:
        """Entry point for RawStep."""
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
                "source_type": context.source_type,
                "config": service.config, # Connection params
                "account_id": service.account_id
            } 
            for unit in work_units
        ]

        # 3. Create the distributed Ray Dataset
        ds = ray.data.from_items(task_payloads)

        # 3. Define the extraction task (Runs in parallel on Ray Workers)
        def fetch_task(item: dict[str, Any]) -> pl.DataFrame:
            """This function runs on the Ray Worker (K8S Pod)."""
            # 1. Importing this triggers the __init__.py discovery logic 
            # and populates ServiceFactory.registry automatically.
            from src.services.factory import ServiceFactory
            
            # 2. Get the singleton instance for this process
            # It will use the Diskcache to check if it's allowed to run.
            service = ServiceFactory.get_service(
                item["source_type"], 
                item["account_id"], 
                **item["config"]
            )
            return pl.concat(list(service.fetch_df(item["unit"])))

        # 4. Map the task across the cluster
        # .iter_batches() makes this a generator!
        ray_dataset = ds.map(fetch_task)
        
        # 5. Yield blocks back to IngestionStream
        # Ray handles backpressure here: it only fetches the next block 
        # when IngestionStream is ready for it.
        for batch in ray_dataset.iter_batches(batch_format="polars"):
            yield batch
    
    def to_parquet(
        self, 
        generator: Generator[pl.DataFrame, None, None], 
        destination: Path
    ) -> List[Dict[str, Any]]:
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
            
            # Capture metadata for the RawStep to process
            metadata_list.append({
                "path": file_path,
                "rows": len(df),
                "schema": df.schema
            })
            
        return metadata_list
    
    @abstractmethod
    def get_work_units(self, client: Any, context: ReaderContext) -> List[str]:
        pass

class DBDataReader(DataReader):
    def get_work_units(self, client: Any, context: ReaderContext) -> List[str]:
        # Uses ORA_HASH for Oracle or ctid for Postgres
        # to generate N unique queries for the 50M rows
        return client.get_load_strategy(
            table_name=context.target_table, 
            partitions=context.parallelism
        )

class ReaderFactory:
    @staticmethod
    def get_strategy(source_type: str) -> Reader:
        strategies = {
            "data": DataReader(),
            "file": FileReader(),
        }
        if source_type not in strategies:
            raise ValueError(f"Unsupported source type: {source_type}")
        return strategies[source_type]
