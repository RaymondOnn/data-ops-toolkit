from pathlib import Path

from loguru import logger
from msgspec import Struct

from apps.ingestion.src.services.base import Sink

LOG = logger


class LoadContext(Struct):
    sink_identifier: str  # Table name or S3 Prefix
    partition_col: str
    partition_value: str
    expected_count: int


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
        self,
        service: Sink,
        source_dir: Path,
        load_ctx: LoadContext,
        file_ext: str = "parquet",
    ) -> tuple[str, int]:
        """
        Phase 1: Moves data from Silver (Parquet) to a temporary 'Staging' area.
        Returns metadata about the staged data (staging_artifact, rows_loaded).
        """
        try:
            LOG.info(
                "Staging data into {table} for {partition_col}={partition_val}",
                table=load_ctx.sink_identifier,
                partition_col=load_ctx.partition_col,
                partition_val=load_ctx.partition_value,
            )
            result = service.stage_data(
                source_dir=source_dir,
                target_table=load_ctx.sink_identifier,
                file_ext=file_ext,
                expected_count=load_ctx.expected_count,
            )

            if result is None:
                raise ValueError(
                    f"Service {type(service).__name__} returned None for staging results. "
                    "Ensure the service implementation returns (staging_identifier, row_count)."
                )
            return result
        except Exception as exc:
            LOG.exception("Error during staging data")
            raise exc

    def promote(
        self, service: Sink, staging_identifier: str, load_ctx: LoadContext
    ) -> None:
        """
        Phase 2: Moves data from 'Staging' to the 'Production' destination.
        This is where 'Atomic Swaps' or 'Merges' happen.
        """
        LOG.info(
            "Promoting data from {from_table} to {to_table} "
            "for {partition_col}={partition_val}",
            from_table=staging_identifier,
            to_table=load_ctx.sink_identifier,
            partition_col=load_ctx.partition_col,
            partition_val=load_ctx.partition_value,
        )
        service.promote_data(
            staging_table=staging_identifier,
            target_table=load_ctx.sink_identifier,
            partition_col=load_ctx.partition_col,
            partition_val=load_ctx.partition_value,
            expected_count=load_ctx.expected_count,
        )
