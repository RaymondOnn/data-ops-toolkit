"""Buffers state updates and streams them to ClickHouse."""

from datetime import datetime
from pathlib import Path
from threading import RLock

import msgspec
import polars as pl
from apps.ingestion.src.core.orchestrator.enums import TaskRecord
from apps.ingestion.src.services.factory import ServiceFactory
from libs.database import TypeResolver
from loguru import logger

LOG = logger
DESTINATION_TBL = "META.EXECUTION_LOG"


class StateSink:
    """Manages the spill-over buffer and SQL streaming for state telemetry.

    The stream ensures that state updates are persisted to disk
    immediately and periodically batch-loaded into ClickHouse for
    historical analysis.
    """

    def __init__(self, db_config: dict, workspace_dir: Path, flush_threshold: int = 50):
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
        self.db_config = db_config
        self.workspace_dir = workspace_dir
        self.stage_dir = workspace_dir / "staging"
        self.archive_dir = workspace_dir / "archive"
        self.flush_threshold = flush_threshold

        self._buffer: list[TaskRecord] = []
        self._buffer_lock = RLock()
        self.stream_path = workspace_dir / "execution_stream.jsonl"

        self._db = None
        self._target_schema = None

        self.stage_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        LOG.info(
            "StateStream initialized",
            stage_dir=str(self.stage_dir),
            archive_dir=str(self.archive_dir),
            flush_threshold=flush_threshold,
        )

    def append(self, record: TaskRecord) -> None:
        """Adds a record to the in-memory buffer.

        Args:
            record: The TaskRecord to stream.

        Decision: Threshold Trigger.
        To minimize disk contention, we only write the JSONL file once
        the buffer reaches the flush_threshold. This balances
        durability with system performance.
        """
        with self._buffer_lock:
            self._buffer.append(record)
            buffer_size = len(self._buffer)
            LOG.trace(
                "Appended to buffer", run_id=record.RUN_ID, buffer_size=buffer_size
            )

            if buffer_size >= self.flush_threshold:
                LOG.debug(
                    "Buffer threshold reached, flushing to disk",
                    buffer_size=buffer_size,
                )
                self._flush_to_disk()

    def _flush_to_disk(self, force: bool = False) -> None:
        """Appends buffered records to the local JSONL stream file.

        Args:
            force: If True, ignores threshold and flushes immediately.
        """
        with self._buffer_lock:
            if not self._buffer:
                return

            if not force and len(self._buffer) < self.flush_threshold:
                return

            batch = b"".join(
                msgspec.json.encode(r.to_dict()) + b"\n" for r in self._buffer
            )
            with self.stream_path.open("ab") as f:
                f.write(batch)

            LOG.info("Flushed records to JSONL", count=len(self._buffer), forced=force)
            self._buffer.clear()

    def _spill_over_to_file(self) -> Path | None:
        """Rotates the primary stream file into a timestamped batch file.

        Returns:
            Path | None: The path to the rotated batch file if data existed.

        Decision: File Rotation.
        We rename the active stream file before conversion. This
        allows the StateStore to continue appending new records to a
        fresh file while the database loader processes the previous batch
        in isolation.
        """
        self._flush_to_disk(force=True)

        if not self.stream_path.exists() or self.stream_path.stat().st_size == 0:
            LOG.debug("No data to rotate", path=str(self.stream_path))
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        rotated = self.stage_dir / f"batch_{timestamp}.jsonl"
        self.stream_path.rename(rotated)

        LOG.info(
            "Spill over logs to new file",
            source=str(self.stream_path),
            destination=str(rotated),
        )
        return rotated

    @property
    def db(self):
        """Lazy-loaded database client connection.

        Decision: Lazy Initialization.
        Database handles are established only when a 'send' operation
        is triggered. This reduces the number of idle connections
        held by short-lived CLI processes.
        """
        if self._db is None:
            print(self.db_config)
            config = self.db_config.copy()
            service_type = config.pop("type")
            LOG.debug("Initializing database client", service_type=service_type)
            self._db = ServiceFactory.get(service_type=service_type, **config)
        return self._db

    def _get_schema(self) -> pl.DataFrame:
        """Retrieves and caches the column schema from the database.

        Returns:
            pl.DataFrame: The schema metadata.
        """
        if self._target_schema is None:
            LOG.debug("Fetching target table schema", table=DESTINATION_TBL)
            self._target_schema = self.db.client.get_schema(DESTINATION_TBL)
        return self._target_schema

    def send(self) -> bool:
        """Converts local stream batches to Parquet and uploads to the database.

        Returns:
            bool: True if data was successfully sent.

        Decision: Transactional Flow.
        We perform a full rotation-conversion-load cycle. If any step
        fails, the original JSONL data is preserved in the staging
        directory, allowing for automatic recovery on the next attempt.
        """
        jsonl_path = self._spill_over_to_file()
        if not jsonl_path:
            LOG.debug("No data to send")
            return False

        try:
            LOG.info("Sending state data", source=str(jsonl_path))
            parquet_path = self._convert_to_parquet(jsonl_path)
            if not parquet_path:
                LOG.warning("No valid records to send", path=str(jsonl_path))
                return False

            self._load_to_database()
            LOG.success("Successfully sent state data")
            return True

        except Exception:
            LOG.exception("Send failed")
            raise

    def _convert_to_parquet(self, jsonl_path: Path) -> Path | None:
        """Converts a raw JSONL file into an aligned, typed Parquet file.

        Args:
            jsonl_path: Path to the source JSONL batch.

        Returns:
            Path | None: The path to the generated Parquet file.

        Decision: Schema Enforcement.
        JSONL is inherently untyped. By using the database's own
        schema to drive the Polars conversion, we ensure that every
        field (especially timestamps and bitmasks) is correctly cast
        before reaching the database, avoiding bulk-load failures.
        """
        schema_df = self._get_schema()
        scan_schema = {row["column_name"]: pl.String for row in schema_df.to_dicts()}

        LOG.debug("Scanning JSONL with explicit schema", path=str(jsonl_path))
        lf = pl.scan_ndjson(jsonl_path, schema=scan_schema)
        lf_cols = {c.upper(): c for c in lf.columns}

        expressions = [
            self._build_cast_expr(row, lf_cols) for row in schema_df.to_dicts()
        ]

        # Execute and ensure we have a DataFrame
        result = lf.select(expressions).collect()

        # Polars can return different types; ensure we have a DataFrame
        if not isinstance(result, pl.DataFrame):
            LOG.error("Expected DataFrame but got", type=type(result).__name__)
            jsonl_path.unlink()
            return None

        # Now safe to access .height and .write_parquet
        if result.height == 0:
            LOG.warning("No records after conversion", path=str(jsonl_path))
            jsonl_path.unlink()
            return None

        parquet_path = jsonl_path.with_suffix(".parquet")
        result.write_parquet(parquet_path)
        jsonl_path.unlink()

        LOG.info("Converted to Parquet", rows=result.height, path=str(parquet_path))
        return parquet_path

    def _build_cast_expr(self, row: dict[str, str], lf_cols: dict[str, str]) -> pl.Expr:
        """Builds a Polars expression to cast and align a column to the DB schema.

        Decision: Type Normalization.
        We handle date and datetime strings explicitly with
        strict=False. This prevents 'junk' data in the log
        (e.g., malformed timestamps) from crashing the entire state
        sync, instead gracefully defaulting to NULL.
        """
        col_name = row["column_name"]
        db_col_upper = col_name.upper()
        target_type = TypeResolver.resolve_to_polars("clickhouse", row["data_type"])

        if db_col_upper not in lf_cols:
            LOG.trace("Column not found in source, using NULL", column=col_name)
            return pl.lit(None).cast(target_type).alias(col_name)

        expr = pl.col(lf_cols[db_col_upper]).cast(pl.String)
        dtype = row["data_type"].casefold()

        if "datetime" in dtype:
            expr = expr.str.to_datetime(strict=False)
        elif "date" in dtype:
            expr = expr.str.to_date(format="%Y-%m-%d", strict=False)

        return expr.cast(target_type, strict=False).alias(col_name)

    def _load_to_database(self) -> None:
        """Load all staged Parquet files to ClickHouse."""
        pending = list(self.stage_dir.glob("*.parquet"))
        if not pending:
            LOG.debug("No pending Parquet files to load")
            return

        LOG.info("Loading Parquet files to ClickHouse", count=len(pending))

        self.db.client.copy_from_file(
            table=DESTINATION_TBL,
            source_dir=str(self.stage_dir),
            file_ext="parquet",
        )

        for pq_file in pending:
            pq_file.rename(self.archive_dir / pq_file.name)
            LOG.debug("Archived Parquet file", file=pq_file.name)

    def close(self) -> None:
        """Close stream and cleanup resources."""
        LOG.info("Closing StateStream")
        try:
            self.send()
        except Exception:
            LOG.exception("Final send failed")

        if self._db and hasattr(self._db, "close"):
            self._db.close()
            LOG.debug("Closed database connection")
