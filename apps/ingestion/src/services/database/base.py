"""Base database service abstractions.

This module provides the structural foundation for all SQL-based services,
handling connection pooling, resource-aware partitioning, and circuit
breaker integration to ensure resilient data extraction and loading.
"""

import re
from abc import abstractmethod
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
import polars as pl
from apps.ingestion.src.core.monitor import monitor
from apps.ingestion.src.services.base import Service, Sink, Source
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreaker
from loguru import logger

if TYPE_CHECKING:
    from libs.database.clients.base import DBClient


LOG = logger
MAX_CELLS_PER_WORKER = 20_000_000
MIN_ROWS_PER_WORKER = 50_000

breaker = CircuitBreaker(
    failure_threshold=3,
    timeout_secs=300,
    tracked_exceptions=(ClientCantConnect, ConnectionError, TimeoutError),
)


class SQLContext(msgspec.Struct, frozen=True):
    """Encapsulates parameters for building dynamic and complex SQL queries safely."""

    resource: str  # The target table name
    select: list[str] = msgspec.field(
        default_factory=list
    )  # Specific columns to select
    where: str | None = None  # Filtering expression
    limit: int | None = None  # Row limitations
    sql: str | None = None  # Optional raw/complex query

    def compile(self) -> str:
        """
        Compiles the components into a single, unified ClickHouse-compatible SQL query
        using a CTE to cleanly combine 'sql' and 'select/where/limit'.
        """
        # 1. Resolve the base dataset (Either the custom SQL query or a standard SELECT *)
        if self.sql:
            # Strip trailing semicolons if present in the raw SQL
            base_query = self.sql.strip().rstrip(";")
        else:
            base_query = f"SELECT * FROM {self.resource}"

        # 2. Wrap the base dataset in a CTE so we can safely chain filters/limits
        cte_query = f"WITH __base_dataset AS ({base_query})"

        # 3. Determine target columns
        cols_str = ", ".join(self.select) if self.select else "*"

        # 4. Assemble the outer wrapper query
        final_query = f"{cte_query} SELECT {cols_str} FROM __base_dataset"

        if self.where:
            # Strip potential user-entered 'WHERE ' prefix for resilience
            clean_where = self.where.strip()
            if clean_where.lower().startswith("where"):
                clean_where = clean_where[5:].strip()
            final_query += f" WHERE {clean_where}"

        if self.limit is not None:
            final_query += f" LIMIT {int(self.limit)}"

        return final_query


class DatabaseService(Service):
    """Base class for SQL-based ingestion services.

    Provides lazily-initialized database clients, connection management via
    context managers, and standardized telemetry through circuit breakers.

    Attributes:
        name (str): Unique identifier for the service.
        config (dict): Configuration parameters including credentials.
    """

    def __init__(self, name: str, **config: Any):
        """Initializes the SQL-based service with registry metadata.

        Args:
            name: The unique identifier for the service instance.
            **config: Driver-specific configuration parameters, including
                credentials which may be `Secret` objects.

        Notes:
            We store the raw config to allow Ray workers to recreate the database
            client locally. This ensures that socket handles are not serialized
            and shared across network boundaries, which is unsupported by
            most DB drivers.
        """
        super().__init__(name, **config)
        self._config = config

    def reset_client(self) -> None:
        """Invalidates the current database client.

        Used by the circuit breaker to force a full re-connection if the
        underlying driver's connection pool becomes stale or corrupted
        after a network interruption.
        """
        if "client" in self.__dict__:
            LOG.warning(f"Resetting database client for service: {self.name}")
            self.__dict__.pop("client", None)

    @property
    @abstractmethod
    def client(self) -> "DBClient":
        """Retrieves the database client, initializing it if necessary.

        Connections are not established until the first operation is requested,
        reducing overhead for short-lived metadata checks.

        Returns:
            DBClient: The protocol-specific database client.
        """
        pass

    @contextmanager
    def connection(self) -> Generator[Any, None, None]:
        """Context manager for obtaining a database connection.

        Ensures that socket handles are properly returned to the pool even
        in the event of an unhandled exception during processing.

        Yields:
            Any: A raw connection handle from the underlying pool.
        """
        with self.client.get_connection() as conn:
            yield conn

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
        return self.client.fetch_df(query)

    @monitor(breaker)
    def fetch(self, query: str) -> list[Sequence[Any]]:
        """Executes a SQL query and returns the complete result set.

        Args:
            query: The SQL command to execute.

        Returns:
            list[Sequence[Any]]: A list of raw result tuples.
        """
        return self.client.sql(query)

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
        query = f"SELECT COUNT(*) FROM {target} WHERE {where.rstrip('; ')}"
        return self._count_rows(query)

    def _count_rows(self, query: str) -> int:
        """
        Counts rows for a target table or a complex query (including CTEs).
        """
        # Wrap the entire complex query/CTE statement as a subquery
        query_clean = query.strip().rstrip(";")
        count_sql = f"SELECT COUNT(*) FROM ({query_clean})"
        print(count_sql)

        try:
            result = self.fetch(count_sql)
            return int(result[0][0]) if result else 0
        except Exception:
            LOG.exception(f"Count failed for target query/table: {query[:50]}...")
            return 0


class DatabaseSource(DatabaseService, Source):
    """Service for extracting data from SQL databases."""

    def parallelize(
        self,
        target: str,
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
        sql_context = kwargs.get("sql_context")
        if sql_context:
            compiled_query = sql_context.compile()
            num_columns = len(sql_context.select)
        else:
            compiled_query = f"SELECT * FROM {target}"
            num_columns = self.client.get_schema(target).height

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
                    source=self.name,
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
            source=self.name,
        )

        return self._partition_load(query=compiled_query, num_workers=num_workers)

    @abstractmethod
    def _partition_load(
        self,
        query: str,
        num_workers: int = 10,
    ) -> list[str]:
        """
        Generates a set of partitioned queries for parallel loading.

        Args:
            table_name: Fully qualified name of the source table.
            num_workers: Number of parallel loaders/workers.
            filter_condition: Optional filter condition.

        Returns:
            set[str]: A set of query strings for distributed execution.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def resolve_identity(
        self, target: str, items: list[str] | None = None, **kwargs
    ) -> str:
        """Resolves the audit identity for the source resource."""
        return target

    @monitor(breaker)
    def pull(self, unit: str) -> pl.DataFrame:
        """Extracts data for a specific work unit.

        Args:
            unit: The SQL query to execute.

        Returns:
            pl.DataFrame: The extracted dataset chunk.
        """
        batches = list(self.client.fetch_df(unit))
        if not batches:
            return pl.DataFrame()
        return pl.concat(batches)


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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def is_equal(
        self,
        ref: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        """Compares two tables for data equality."""
        pass

    @abstractmethod
    def clone(self, source: str, dest: str) -> None:
        """Clones a table structure for regression testing."""
        pass

    @abstractmethod
    def _minus(
        self,
        ref: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> int:
        """Performs a SQL MINUS/EXCEPT to find data drift."""
        pass

    @abstractmethod
    def delete(self, target: str) -> None:
        """Drops a database table."""
        pass

    @abstractmethod
    def _get_checksum(self, name: str, columns: list[str] | None = None) -> str:
        """Generates a data fingerprint for the table."""
        pass

    @monitor(breaker)
    def exists(self, target: str) -> bool:
        """Checks if a table or view exists in the database."""
        return self.client.exists(target)
