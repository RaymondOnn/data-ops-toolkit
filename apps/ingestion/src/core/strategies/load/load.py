from typing import TYPE_CHECKING, TypeAlias
from pathlib import Path

from msgspec import Struct # type: ignore
import structlog # type: ignore

from src.services.database import DatabaseService
from src.services.file import StorageService

LOG = structlog.getLogger(__name__)

if TYPE_CHECKING:
    from src.services.database import Service

Sink: TypeAlias = DatabaseService | StorageService

class WriteContext(Struct):
    target_destination: str                  # Table name or S3 Prefix
    partition_col: str | None = None
    partition_value: str | None = None
    
class StagingResult(Struct):
    staging_path: str | None = None  # For S3 or local Parquet
    staging_table: str | None = None # For DB Temp tables
    rows: int = 0


class Loader:
    """
    Defines the behavioral contract for moving data into production.
    Each implementation (Append, Upsert, Overwrite) handles the 
    logic for both Staging and Promotion.
    """
    def load(self, service: Sink, source_dir: Path | str, target_table: str) -> StagingResult:
        """
        Phase 1: Moves data from Silver (Parquet) to a temporary 'Staging' area.
        Returns metadata about the staged data (rows, temp_path/temp_table).
        """
        LOG.info("staging_started", table=target_table)
        temp_table = service.stage_data(source_dir, target_table)
        return StagingResult(staging_table=temp_table)


    def promote(self, service: Sink, staging_info: StagingResult, write_ctx: WriteContext) -> None:
        """
        Phase 2: Moves data from 'Staging' to the 'Production' destination.
        This is where 'Atomic Swaps' or 'Merges' happen.
        """
        staging_table = staging_info.staging_table or staging_info.staging_path
        
        LOG.info(
            "promotion_started", 
            from_table=staging_table, 
            to_table=write_ctx.target_destination
        )
        service.promote_data(
            staging_table=staging_table, 
            target_table=write_ctx.target_destination, 
            partition_col=write_ctx.partition_col, 
            partition_val=write_ctx.partition_value
        )
        
