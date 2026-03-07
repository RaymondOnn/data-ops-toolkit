from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import polars as pl
from msgspec import Struct
import structlog

LOG = structlog.getLogger(__name__)

if TYPE_CHECKING:
    from src.services.database import Service



class WriteContext(Struct):
    target_destination: str                  # Table name or S3 Prefix
    load_mode: str               # upsert, overwrite, append
    partition_col: str | None = None
    partition_value: str | None = None
    
class StagingResult(Struct):
    staging_path: str | None = None  # For S3 or local Parquet
    staging_table: str | None = None # For DB Temp tables
    rows: int = 0


class LoadFactory:
    """
    Resolves the correct behavioral strategy for the data load.
    Example: (Sink: S3 + Mode: Overwrite) -> S3OverwriteStrategy
    """
    
    # Map of (Destination Type, Load Mode) -> Strategy Class
    _REGISTRY = {
        # Database Sinks
        ("postgres", "append"): PostgresAppendStrategy,
        ("postgres", "overwrite"): PostgresOverwriteStrategy,
        ("postgres", "upsert"): PostgresUpsertStrategy,
        
        # Object Storage Sinks
        ("s3", "append"): S3AppendStrategy,
        ("s3", "overwrite"): S3OverwriteStrategy,
        
        # Snowflake Sinks
        ("snowflake", "upsert"): SnowflakeUpsertStrategy,
    }

    @classmethod
    def get_loader(cls, sink_type: str, mode: str) -> Loader:
        # Normalize inputs
        lookup = (sink_type.lower(), mode.lower())
        
        strategy_class = cls._REGISTRY.get(lookup)
        
        if not strategy_class:
            # Provide a helpful error if the combination isn't supported
            supported = ", ".join([f"{s}/{m}" for s, m in cls._REGISTRY.keys()])
            raise NotImplementedError(
                f"No strategy found for {sink_type} with mode '{mode}'. "
                f"Supported combinations: {supported}"
            )
            
        return strategy_class()



class Loader(ABC):
    """
    Defines the behavioral contract for moving data into production.
    Each implementation (Append, Upsert, Overwrite) handles the 
    logic for both Staging and Promotion.
    """
    
    @abstractmethod
    def load(self, service: Service, lf: pl.LazyFrame, target: str) -> StagingResult:
        """
        Phase 1: Moves data from Silver (Parquet) to a temporary 'Staging' area.
        Returns metadata about the staged data (rows, temp_path/temp_table).
        """
        pass

    @abstractmethod
    def promote(self,service: Service, staging_info: dict, target: str) -> None:
        """
        Phase 2: Moves data from 'Staging' to the 'Production' destination.
        This is where 'Atomic Swaps' or 'Merges' happen.
        """
        pass

class UpsertLoader(Loader):
    def load(self, service: Service, lf: pl.LazyFrame, target: str) -> dict:
        """Phase 1: The Staging Phase."""
        LOG.info("staging_started", table=target)
        temp_table = service.stage_data(lf, target)
        return StagingResult(staging_table=temp_table)


    def promote(self, service: Service, staging_info: dict, target: str):
        """Phase 2: The Swap Phase."""
        staging_table = staging_info["staging_table"]
        LOG.info("promotion_started", from_table=staging_table, to_table=target)
        service.promote_data(staging_table, target, mode="upsert")
        
class OverwriteLoader(Loader):
    def load(self, service: Service, lf: pl.LazyFrame, context: WriteContext) -> StagingResult:
        # Move data to intermediate area
        staging_info = service.stage_data(lf, context.target)
        return staging_info

    def promote(self, service: Service, result: StagingResult, context: WriteContext):
        # The service implementation of promote_data handles the logic:
        # e.g. "DELETE FROM production WHERE partition_col = partition_value"
        # followed by "INSERT INTO production SELECT * FROM staging"
        service.promote_data(
            staging_area=result.staging_table or result.staging_path,
            target=context.target,
            mode="overwrite",
            partition_col=context.partition_col,
            partition_value=context.partition_value
        )