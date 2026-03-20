from pathlib import Path
from typing import TypeAlias

import structlog
from msgspec import Struct
from src.services.database import DatabaseService
from src.services.file import StorageService

LOG = structlog.getLogger(__name__)


Sink: TypeAlias = DatabaseService | StorageService


class WriteContext(Struct):
    sink_identifier: str  # Table name or S3 Prefix
    partition_col: str
    partition_value: str


class StagingResult(Struct):
    staging_path: str | None = None  # For S3 or local Parquet
    staging_table: str | None = None  # For DB Temp tables
    rows: int = 0


class Loader:
    """
    Defines the behavioral contract for moving data into production.
    Each implementation (Append, Upsert, Overwrite) handles the
    logic for both Staging and Promotion.
    """

    def load(
        self, service: Sink, source_dir: Path, target_table: str
    ) -> StagingResult:
        """
        Phase 1: Moves data from Silver (Parquet) to a temporary 'Staging' area.
        Returns metadata about the staged data (rows, temp_path/temp_table).
        """
        LOG.info("staging_started", table=target_table)
        temp_table = service.stage_data(source_dir, target_table)
        return StagingResult(staging_table=temp_table)

    def promote(
        self, service: Sink, staging_identifier: str, write_ctx: WriteContext
    ) -> None:
        """
        Phase 2: Moves data from 'Staging' to the 'Production' destination.
        This is where 'Atomic Swaps' or 'Merges' happen.
        """
        LOG.info(
            "promotion_started",
            from_table=staging_identifier,
            to_table=write_ctx.sink_identifier,
        )
        service.promote_data(
            staging_table=staging_identifier,
            target_table=write_ctx.sink_identifier,
            partition_col=write_ctx.partition_col,
            partition_val=write_ctx.partition_value,
        )
