"""Buffers state updates and streams them to ClickHouse."""

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import msgspec
import polars as pl
from libs.file.formats.json import JSONHandler
from libs.file.formats.parquet import ParquetHandler
from loguru import logger

from .models import TaskUpdate

if TYPE_CHECKING:
    from src.services.repo.metadata import MetadataRepository

LOG = logger
DESTINATION_TBL = "META.EXECUTION_LOG"


class StateSink:
    """Manages the spill-over buffer and SQL streaming for state telemetry.

    The stream ensures that state updates are persisted to disk
    immediately and periodically batch-loaded into ClickHouse for
    historical analysis.
    """

    def __init__(
        self,
        meta_repo: "MetadataRepository",
        workspace_dir: Path,
        flush_threshold: int = 50,
    ):
        """Initializes the stream buffer and staging directories.

        Args:
            db_config: Connectivity details for the database.
            workspace_dir: Base directory for stream files.
            flush_threshold: Number of records before an automatic disk flush.

        Decision: Hierarchical Storage.
        We maintain 'staging' and 'archive' directories. Staging holds
        final Parquet files awaiting load, ensuring that state is
        never lost if the database is temporarily unreachable.
        """
        self.meta_repo = meta_repo
        self.workspace_dir = workspace_dir

        self.state_dir = workspace_dir / "state"
        self.stage_dir = self.state_dir / "staging"
        self.archive_dir = self.state_dir / "archive"

        for d in [self.state_dir, self.stage_dir, self.archive_dir]:
            d.mkdir(parents=True, exist_ok=True)

        self.stream_file = workspace_dir / "execution_stream.jsonl"

        self.json_handler = JSONHandler()
        self.parquet_handler = ParquetHandler()

        LOG.trace(
            "StateSink initialized",
            stage_dir=str(self.stage_dir),
            archive_dir=str(self.archive_dir),
            flush_threshold=flush_threshold,
        )

    def _rotate_and_stage(self) -> Path | None:
        """Rotates active stream file to a uniquely named staging batch."""
        if not self.stream_file.exists() or self.stream_file.stat().st_size == 0:
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid4().hex[:6]
        staged_path = self.stage_dir / f"batch_{timestamp}_{unique_id}.jsonl"

        # Rename active file to staging path
        self.stream_file.rename(staged_path)
        return staged_path

    def append(self, update: TaskUpdate) -> None:
        """
        Directly appends a TaskUpdate line to execution_stream.jsonl.
        Synchronous, simple, and relies on standard OS filesystem write-buffers.
        """
        try:
            with self.stream_file.open("a", encoding="utf-8") as f:
                f.write(msgspec.json.encode(update).decode("utf-8") + "\n")
        except Exception:
            LOG.exception(
                f"Failed to append update for RUN_ID={update.JOB_ID} to stream file."
            )

    def flush_to_db(self, target_table: str = "META.EXECUTION_LOG") -> None:
        """
        Uses JSONHandler and ParquetHandler to stage, convert, and stream
        buffered updates into ClickHouse.
        """
        if not self.meta_repo:
            LOG.warning(
                "MetadataRepository is not initialized on StateSink. Bypassing flush_to_db."
            )
            return

        staged_jsonl = self._rotate_and_stage()
        if not staged_jsonl:
            LOG.trace("No staged stream file available for state flush.")
            return

        # Prepare staging subfolder expected by MetadataRepository.bulk_load_parquet
        staged_batch_dir = self.stage_dir / f"batch_{staged_jsonl.stem}"
        staged_batch_dir.mkdir(exist_ok=True)
        parquet_dst = staged_batch_dir / f"{staged_jsonl.stem}.parquet"

        try:
            # 1. Read JSONL file to Polars LazyFrame using JSONHandler
            lazy_df = self.json_handler.to_df(str(staged_jsonl))

            # 2. Sanitize struct columns (like RUNTIME_OVERRIDES) to JSON Strings
            schema = lazy_df.collect_schema()
            struct_cols = [
                col for col, dtype in schema.items() if isinstance(dtype, pl.Struct)
            ]
            if struct_cols:
                lazy_df = lazy_df.with_columns(
                    [pl.col(c).struct.json_encode() for c in struct_cols]
                )

            # 3. Convert & Write to Parquet using ParquetHandler
            self.parquet_handler.from_df(lazy_df, str(parquet_dst))

            # 4. Stream Parquet file to ClickHouse using MetadataRepository
            self.meta_repo.bulk_load_parquet(target_table, parquet_dst)

            # 5. Clean up batch subfolder and archive raw JSONL file
            staged_jsonl.rename(self.archive_dir / staged_jsonl.name)
            parquet_dst.rename(self.archive_dir / parquet_dst.name)
            staged_batch_dir.rmdir()

        except Exception:
            LOG.exception(f"Failed to flush batch {staged_jsonl.name} to database.")
