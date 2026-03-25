from pathlib import Path

import structlog
from apps.ingestion.src.services.base import SinkMixin
from msgspec import Struct

LOG = structlog.getLogger(__name__)


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
        self, service: SinkMixin, source_dir: Path, target_table: str
    ) -> tuple[str, int]:
        """
        Phase 1: Moves data from Silver (Parquet) to a temporary 'Staging' area.
        Returns metadata about the staged data (staging_artifact, rows_loaded).
        """
        LOG.info("staging_started", table=target_table)
        return service.stage_data(source_dir, target_table)

    def promote(
        self, service: SinkMixin, staging_identifier: str, write_ctx: WriteContext
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
