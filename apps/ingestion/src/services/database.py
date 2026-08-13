"""Base database service abstractions.

This module provides the structural foundation for all SQL-based services,
handling connection pooling, resource-aware partitioning, and circuit
breaker integration to ensure resilient data extraction and loading.
"""

import re
from abc import abstractmethod
from collections.abc import Generator
from copy import deepcopy
from pathlib import Path
from typing import Any

import polars as pl
from libs.auth.secret import Secret
from libs.clients.base import ClientCantConnect
from libs.database import DatabaseConnector
from libs.database.sql import SQLContext
from libs.resilience.circuit_breaker import CircuitBreaker
from libs.utils.dates import current_timestamp
from loguru import logger

from src.services.base import Service, Sink, Source
from src.services.factory import ServiceFactory
from src.services.health.monitor import monitor

LOG = logger
MAX_CELLS_PER_WORKER = 20_000_000
MIN_ROWS_PER_WORKER = 50_000

breaker = CircuitBreaker(
    failure_threshold=3,
    timeout_secs=300,
    tracked_exceptions=(ClientCantConnect, ConnectionError, TimeoutError),
)


@ServiceFactory.register(source_type="database")
class DatabaseService(Service):
    """Base class for SQL-based ingestion services.

    Provides lazily-initialized database clients, connection management via
    context managers, and standardized telemetry through circuit breakers.

    Attributes:
        config (dict): Configuration parameters including credentials.
    """

    def __init__(self, name: str | None = None, **config: Any):
        """Initializes the SQL-based service with registry metadata.

        Args:
            name: Name of the service instance.
            **config: Driver-specific configuration parameters, including
                credentials which may be `Secret` objects.
        """
        self.name = name or config.get("type") or self.__class__.__name__.lower()
        self._config = config

    def probe(self) -> bool:
        """Health probe for database connector."""
        try:
            res = self.connector.query("SELECT 1")
            return bool(res)
        except Exception as e:
            LOG.warning(f"Database health probe failed for {self.name}: {e}")
            return False

    def reset_client(self) -> None:
        """Invalidates the current database client.

        Used by the circuit breaker to force a full re-connection if the
        underlying driver's connection pool becomes stale or corrupted
        after a network interruption.
        """
        if "client" in self.__dict__:
            LOG.warning(f"Resetting database client for {self.__class__.__qualname__}")
            self.__dict__.pop("client", None)

    @property
    def connector(self) -> "DatabaseConnector":
        """Retrieves the database client, initializing it if necessary.

        Connections are not established until the first operation is requested,
        reducing overhead for short-lived metadata checks.

        Returns:
            DBClient: The protocol-specific database client.
        """
        resolved_config = deepcopy(self._config)
        for key, value in self._config.items():
            if isinstance(value, Secret):
                resolved_config[key] = value.resolve(url_encode=True)

        return DatabaseConnector(dialect=self._config["db_type"], **resolved_config)

    # @contextmanager
    # def connection(self) -> Generator[Any, None, None]:
    #     """Context manager for obtaining a database connection.

    #     Ensures that socket handles are properly returned to the pool even
    #     in the event of an unhandled exception during processing.

    #     Yields:
    #         Any: A raw connection handle from the underlying pool.
    #     """
    #     with self.connector.get_connection() as conn:
    #         yield conn

    @monitor(breaker)
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """Streams query results as a sequence of Polars DataFrames.

        By yielding batches instead of a single object, we can process datasets
        larger than the worker's RAM without triggering OOM events.

        Args:
            query: The SQL query to execute.

        Yields:
            pl.DataFrame: A chunk of the result set.
        """
        return self.connector.fetch_df(query)

    def count_units(self, target: str, filter_condition: str | None = None) -> int:
        """Abstract method for determining resource volume (row count).

        Args:
            target: The resource identifier (table name).
            filter_condition: Optional SQL WHERE clause.

        Returns:
            int: Total row count.
        """
        where = (
            re.sub(r"(?i)^where\s+", "", filter_condition.strip())
            if filter_condition
            else "1=1"
        )
        where_cond = self.connector.where(where)
        query = self.connector.select(table=target, where_cond=where_cond)
        return self._count_rows(query)

    def _count_rows(self, query: str) -> int:
        """
        Counts rows for a target table or a complex query (including CTEs).
        """
        clean_query = query.strip().rstrip(";")

        # 1. Compile dialect-specific count query
        count_sql = self.connector._build("count", table=clean_query)

        try:
            # 2. Execute via monitored fetch_df generator and grab the first DataFrame batch
            df = next(self.fetch_df(count_sql), None)

            if df is not None and not df.is_empty():
                # 3. Extract single count scalar cleanly using Polars .item()
                return int(df.item(0, 0))

            return 0
        except Exception:
            LOG.exception(f"Count failed for target query/table: {query[:50]}...")
            return 0


@ServiceFactory.register(source_type="database", role="source")
class DatabaseSource(DatabaseService, Source):
    """Service for extracting data from SQL databases."""

    def parallelize(
        self,
        target: str,
        sql_context: SQLContext,
        num_workers: int | None = None,
        filter_condition: str | None = None,
        **kwargs: Any,
    ) -> list[str]:
        """Calculates optimal parallel partitions based on a Cell-Budget heuristic.

        Instead of splitting by row count alone, we calculate the total volume
        of 'cells' (rows x columns). We target ~20M cells per worker, which
        keeps memory usage within the 2GB Ray worker limit while maximizing
        throughput.

        Args:
            target: Table name to partition.
            num_workers: Manual worker count override.
            filter_condition: Optional SQL WHERE clause.
            **kwargs: Contextual params (e.g., 'schema').

        Returns:
            set[str]: A collection of parallel SQL queries.
        """
        if sql_context:
            compiled_query = self.connector.sql.compile_context(sql_context)
            # Safely check for non-empty select columns or fallback to table schema height
            select_cols = sql_context.main.select if sql_context.main else None
            num_columns = (
                len(select_cols)
                if select_cols
                else self.connector.get_schema(target).height
            )
        else:
            compiled_query = self.connector.select(table=target)
            num_columns = self.connector.get_schema(target).height

        total_rows = self._count_rows(compiled_query)
        total_cells = total_rows * num_columns

        if num_workers:
            # If workers are provided, we check if the resulting chunks
            # violate our memory safety ceiling (20M cells).
            cells_per_worker = total_cells // num_workers
            if cells_per_worker > MAX_CELLS_PER_WORKER:
                suggested = total_cells // MAX_CELLS_PER_WORKER
                LOG.warning(
                    f"Manual worker count ({num_workers}) may cause OOM. "
                    f"Each worker will handle {cells_per_worker:_} cells. "
                    f"Suggested workers for this width: {suggested}",
                    source=self.__class__.__qualname__,
                )
        else:
            # 1. Calculate ideal worker count based on cell memory budget
            num_workers = total_cells // MAX_CELLS_PER_WORKER

        # 2. Convert back to rows to ensure whole rows per worker
        if num_workers > 0:
            rows_per_worker = total_rows // num_workers
            if rows_per_worker < MIN_ROWS_PER_WORKER:
                num_workers = total_rows // MIN_ROWS_PER_WORKER

        # 3. Final safety clamping (1 to 100 workers)
        num_workers = max(1, num_workers)

        LOG.info(
            f"DB Partitioning: {total_rows:_} rows, {num_columns} cols "
            f"({total_cells:_} total cells). Using {num_workers} workers.",
            source=self.__class__.__qualname__,
        )

        return self._partition_load(
            table_or_query=compiled_query, num_workers=num_workers
        )

    def _partition_load(
        self,
        table_or_query: str,
        num_workers: int,
        partition_fields: str | list[str] | None = None,
    ):
        """
        Generates partitioned SQL queries using cityHash64 for parallel loading.
        Safely handles both plain tables and complex CTE blocks by wrapping them in subqueries.

        Args:
            query: The fully compiled SQL query string or table expression.
            num_workers: Number of workers/partitions to split across.

        Returns:
            set[str]: A set of partitioned query strings.
        """
        if num_workers <= 0:
            raise ValueError("num_workers must be an integer greater than 0.")

        clean_query = table_or_query.strip().rstrip(";")

        # Wrap the incoming query inside a subquery parentesis block.
        # This prevents CTE declarations from clashing with the outer WHERE clause.
        # Note: Hash primary key / sort keys when possible

        # Format partition key fields into dialect-specific hash expression (e.g. cityHash64("id", "tenant"))
        if partition_fields:
            p_fields = (
                [partition_fields]
                if isinstance(partition_fields, str)
                else partition_fields
            )
        else:
            p_fields = "*"
        hash_expr = self.connector.expr("hash", fields=p_fields)

        queries = []
        for i in range(num_workers):
            partition_cond = {f"{hash_expr} % {num_workers}": i}
            where_cond = self.connector.where(partition_cond)
            compiled_query = self.connector.select(
                table=clean_query, where_cond=where_cond
            )
            queries.append(compiled_query)

        return queries

    def resolve_identity(
        self, target: str, items: list[str] | None = None, **kwargs
    ) -> str:
        """Resolves the audit identity for the source resource."""
        return target

    @monitor(breaker)
    def pull(self, unit: str, **kwargs: Any) -> pl.DataFrame:
        """Extracts data for a specific work unit.

        Args:
            unit: The SQL query to execute.

        Returns:
            pl.DataFrame: The extracted dataset chunk.
        """
        batches = list(self.connector.fetch_df(unit))
        if not batches:
            return pl.DataFrame()
        return pl.concat(batches)


@ServiceFactory.register(source_type="database", role="sink")
class DatabaseSink(DatabaseService, Sink):
    @abstractmethod
    def stage(
        self,
        source_dir: Path,
        target: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        """Phase 1: Loads local artifacts into a temporary database table.

        Args:
            source_dir: Directory containing transformed Parquet files.
            target: Production table name.
            expected_count: Row count verification.
            file_ext: Format of files in source_dir.
            audit_values: Metadata to inject into the table.
        """
        parts = target.split(".", 1)
        db = parts[0] if len(parts) > 1 else None
        table = parts[-1]

        timestamp = current_timestamp(naive=True).strftime("%Y%m%d%H%M%S")
        staging = f"stg_{table}_{timestamp}"
        full_staging = f"{db}.{staging}" if db else staging

        success = False
        try:
            # self.fetch(f"""
            #         CREATE OR REPLACE TABLE {full_staging}
            #         ENGINE = MergeTree()
            #         ORDER BY tuple()
            #         AS {target}
            #     """)
            self.connector.command(
                "like_table", tgt_table=full_staging, src_table=target
            )
            self.connector.copy_from_file(
                table=full_staging,
                source_dir=str(source_dir),
                file_ext=file_ext,
                audit_values=audit_values or {},
            )

            # Verify row count
            select_sql = self.connector.select(table=full_staging)
            rows = self._count_rows(query=select_sql)

            if rows != expected_count:
                raise ValueError(
                    f"Row count mismatch: expected {expected_count}, got {rows}"
                )

            success = True
            return full_staging, rows

        except Exception:
            LOG.exception("Staging failed")
            raise
        finally:
            if not success:
                self.delete(full_staging)
                LOG.warning(f"Cleaned up failed staging: {full_staging}")

    def promote(
        self,
        staging: str,
        target: str,
        partition_on: str,
        partition_value: str,
        expected_count: int,
    ) -> None:
        """Phase 2: Promotes data from staging to production.

        Args:
            staging: The staging table identifier.
            target: The production table identifier.
            partition_on: The column used for partitioning logic.
            partition_value: The value to overwrite.
            expected_count: Final count verification.
        """
        # Validate schema

        target_cols = {
            row[0]: row[1]
            for row in self.connector.query("describe_table", table_name=target)
        }
        staging_cols = {
            row[0]: row[1]
            for row in self.connector.query("describe_table", table_name=staging)
        }

        if target_cols != staging_cols:
            missing = set(target_cols) - set(staging_cols)
            extra = set(staging_cols) - set(target_cols)
            raise ValueError(f"Schema mismatch - missing: {missing}, extra: {extra}")

        success = False
        try:
            # 2 phase upsert
            where_cond = self.connector.where({partition_on: partition_value})
            self.connector.command(
                "merge_delete", tgt_table=target, where_cond=where_cond
            )
            self.connector.command("merge_insert", tgt_table=target, src_table=staging)

            # Verify row count
            select_sql = self.connector.select(table=target, where_cond=where_cond)
            promoted = self._count_rows(query=select_sql)
            if promoted != expected_count:
                raise ValueError(
                    f"Row count mismatch after promotion: "
                    f"expected {expected_count}, got {promoted}"
                )
            success = True
            LOG.success(f"Promoted {partition_on}={partition_value} to {target}")

        finally:
            if success:
                self.delete(staging)
                LOG.info(f"Cleaned up staging: {staging}")

    def is_equal(
        self,
        ref: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        """Compares two tables for data equality."""
        ref_rows = self._count_rows(self.connector.select(table=ref))
        other_rows = self._count_rows(self.connector.select(table=other))
        if ref_rows != other_rows:
            return False
        if self._get_checksum(ref) == self._get_checksum(other):
            return True
        return self._minus(ref, other, exclude_columns) == 0

    def clone(self, source: str, dest: str) -> None:
        """Clones a table structure for regression testing."""
        self.connector.command("like_table", src_table=source, tgt_table=dest)
        LOG.info(f"Cloned {source} -> {dest}")

    def _minus(
        self,
        ref: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> int:
        """Performs a SQL MINUS/EXCEPT to find data drift."""
        exclude = exclude_columns or set()

        cols_ref = {
            row[0].decode() if isinstance(row[0], bytes) else row[0]
            for row in self.connector.query("describe_table", table_name=ref)
        }
        cols_other = {
            row[0].decode() if isinstance(row[0], bytes) else row[0]
            for row in self.connector.query("describe_table", table_name=other)
        }

        common = (cols_ref & cols_other) - exclude
        if not common:
            raise ValueError(
                f"No common columns found. " f"ref: {cols_ref}, other: {cols_other}"
            )

        minus_sql = self.connector._build(
            "minus", ref_table=ref, other_table=other, fields=sorted(common)
        )
        result = self.connector.query("count", table=minus_sql)
        first_row = next(result, None)
        return int(first_row[0]) if first_row else 0

    def delete(self, target: str) -> None:
        self.connector.command("drop_table", table_name=target)
        LOG.warning(f"Dropped table: {target}")

    def _get_checksum(self, name: str, columns: list[str] | None = None) -> str:
        """Generates a data fingerprint for the table."""
        try:
            field = self.connector.expr("hash", fields=columns)
            result = self.connector.query("select", table=name, fields=[field])

            first_row = next(result, None)
            return str(first_row[0]) if first_row else "0"
        except Exception:
            LOG.exception(f"Checksum failed for {name}")
            return "ERROR"

    @monitor(breaker)
    def exists(self, target: str) -> bool:
        """Checks if a table or view exists in the database."""
        result = self.connector.query("table_exists", table_name=target)
        first_row = next(result, None)
        if not first_row:
            return False
        return (
            bool(first_row[0])
            if isinstance(first_row, list | tuple)
            else bool(first_row)
        )
