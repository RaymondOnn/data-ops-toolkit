import contextlib
from abc import abstractmethod
from collections.abc import Generator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from apps.ingestion.src.services.base import Service, Sink, Source
from apps.ingestion.src.services.registry import protect_service
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreaker
from loguru import logger

if TYPE_CHECKING:
    from libs.database.clients.base import DBClient


LOG = logger

breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=300,
    expected_exceptions=(ClientCantConnect, ConnectionError, TimeoutError),
)


class DatabaseService(Service):
    """Intermediate layer for all SQL-based services (Source/Sink).

    Provides common functionalities like client management, connection leasing,
    and default implementations for data fetching and execution, all protected
    by a circuit breaker.
    """

    def __init__(self, name: str, **config: Any):
        """Initializes the SQL-based service with registry metadata.

        Args:
            name: The unique identifier for the service instance.
            **config: Driver-specific configuration parameters, including
                credentials which may be `Secret` objects.

        Decision: Process-Wide Configuration.
        We store the raw config to allow Ray workers to recreate the
        database client locally, ensuring connection handles are not
        shared across network boundaries.
        """
        super().__init__(name, **config)
        self._config = config

    def reset_client(self) -> None:
        """Force-clears the cached client handle.

        Decision: State Recovery.
        This allows the Circuit Breaker to force a full re-initialization
        if the underlying driver's pool becomes corrupted or exhausted.
        """
        if "client" in self.__dict__:
            LOG.warning(f"Resetting database client for service: {self.name}")
            self.__dict__.pop("client", None)

    @property
    @abstractmethod
    def client(self) -> "DBClient":
        """Abstract property for the underlying database client.

        Returns:
            DBClient: An engine-specific database client instance.
        """
        pass

    @contextlib.contextmanager
    def connection(self) -> Generator[Any, None, None]:
        """Exposes the underlying client's pooled connection lease.

        Yields:
            Any: A raw database connection object.

        Decision: Resource Guarding.
        By yielding the connection via a context manager, we guarantee
        that connections are returned to the pool even if the worker
        process crashes or throws a signal.
        """
        with self.client.get_connection() as conn:
            yield conn

    @protect_service(breaker)
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """Executes a query and yields results as Polars DataFrames.

        Args:
            query: The SQL query string.

        Yields:
            pl.DataFrame: A batch of query results.

        Decision: Polars Streaming.
        We yield DataFrames in batches to ensure that the ingestion
        process can handle 50M+ rows without loading the entire result
        set into the driver's memory.
        """
        return self.client.fetch_df(query)

    @protect_service(breaker)
    def fetch(self, query: str) -> list[Sequence[Any]]:
        """
        Executes a query and returns results as raw tuples.

        Args:
            query: The SQL query string.

        Returns:
            list[Sequence[Any]]: A list of row tuples.
        """
        return self.client.sql(query)

    @protect_service(breaker)
    def execute_batch(self, query: str, data: list[dict[str, Any]]) -> None:
        """
        Executes a parameterized query with a batch of data.

        Args:
            query: The parameterized SQL query string.
            data: A list of dictionaries, where each dictionary represents a
                row of data for the query.

        Raises:
            NotImplementedError: If the underlying client does not support
                batch execution.
        """
        if hasattr(self.client, "execute_batch"):
            self.client.execute_batch(query, data)  # type: ignore
        else:
            raise NotImplementedError(
                "Database client does not support batch execution."
            )

    def get_row_count(self, target: str, filter_condition: str | None = None) -> int:
        """
        Returns the total number of rows for a target table/query.

        Args:
            target: The table or query to count rows from.
            filter_condition: Optional WHERE clause to apply.

        Returns:
            int: The total number of rows.

        Raises:
            NotImplementedError: If the method is not implemented by the
                concrete service.
        """
        raise NotImplementedError(
            "Database client must implement get_row_count method."
        )


class DatabaseSource(DatabaseService, Source):
    """Base class for database-backed data sources."""

    def parallelize(
        self,
        target: str,
        num_workers: int | None = None,
        filter_condition: str | None = None,
        **kwargs: Any,
    ) -> set[str]:
        """Calculates parallel SQL queries based on table width and volume.

        Args:
            target: The fully qualified table name.
            num_workers: Optional worker count override.
            filter_condition: Optional SQL WHERE clause.
            **kwargs: Includes 'schema_items' for width calculation.

        Returns:
            set[str]: A set of SQL queries for parallel execution.

        Decision: Resource-Aware Scaling.
        The Database Service is the authority on how to slice a table. We
        calculate a 'Width Factor' (Columns vs Rows) to ensure each parallel
        query fits within the worker's memory limit.

        Decision: Cell-Count Heuristic.
        Standard row-count scaling fails for 'wide' tables. By targeting 20M
        cells per task, we accurately model memory pressure (~200MB-500MB RAM),
        leaving a buffer for transformations.
        """
        # 1. Logic Shift: Optimization based on cell count (DB Specific)
        if not num_workers:
            total_rows = self.get_total_count(target, filter_condition)

            # Width Calculation (Schema columns)
            schema_items = kwargs.get("schema_items", [])
            num_columns = len(schema_items) or 20

            # Apply Cells-per-Worker Heuristic (Targeting 20M cells per task)
            rows_per_worker = max(50_000, 20_000_000 // num_columns)
            num_workers = max(1, min(100, total_rows // rows_per_worker))

            LOG.info(
                f"DB Auto-scaling: {total_rows} rows, {num_columns} cols. "
                f"Targeting {num_workers} parallel queries.",
                service=self.name,
            )

        # 2. Delegate to client to generate the specific SQL (ORA_HASH, etc.)
        return self.client.partition_load(target, num_workers, filter_condition)

    def resolve_identity(
        self, target: str, discovered_items: list[str] | None = None
    ) -> str:
        """Returns the table name as the authoritative audit identifier.

        Args:
            target: The fully qualified table name.
            discovered_items: Unused for database sources.

        Returns:
            str: The terminal table name.

        Decision: Standardized Lineage.
        We strip schema/database prefixes to ensure that the '_source'
        audit column remains consistent even if a table is moved between
        environments (e.g. STG.ORDERS vs PROD.ORDERS).
        """
        return target.rsplit(".", maxsplit=1)[-1]

    @protect_service(breaker)
    def fetch_data(self, unit: str) -> pl.DataFrame:
        """
        Fetches data for a given work unit (SQL query) and returns a Polars
        DataFrame.

        Args:
            unit: The work unit, typically a SQL query string.

        Returns:
            pl.DataFrame: The extracted data.
        """
        # We iterate through the client's generator. If a connection error occurs
        # during streaming, the @protect_service decorator will catch it.
        batches = list(self.client.fetch_df(unit))
        if not batches:
            return pl.DataFrame()
        return pl.concat(batches)


class DatabaseSink(DatabaseService, Sink):
    @abstractmethod
    def stage_data(
        self,
        source_dir: Path,
        target_table: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        """
        Phase 1: Uploads or stages data to a temporary location in the database.

        Args:
            source_dir: The directory containing files to load.
            target_table: The final destination table name.
            expected_count: The number of rows expected to be loaded.
            file_ext: The format of the source files.
            audit_values: Global constants to inject into the staging layer.

        Returns:
            tuple[str, int]: The temporary staging table name and row count.
        """
        pass

    @abstractmethod
    def promote_data(
        self,
        staging_table: str,
        target_table: str,
        partition_col: str,
        partition_val: str,
        expected_count: int,
    ) -> None:
        """
        Phase 2: Moves data from staging to production.

        Args:
            staging_table: The temporary table containing staged data.
            target_table: The destination production table.
            partition_col: The column to use for partitioning/replacement.
            partition_val: The specific partition value to promote.
            expected_count: Verification count for promotion.
        """
        pass

    @abstractmethod
    def is_equal(
        self,
        reference: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        """
        Compares two tables for equality.

        Args:
            reference: The reference table name.
            other: The table to compare against.
            exclude_columns: Optional set of columns to exclude from comparison.

        Returns:
            bool: True if the tables are equal, False otherwise.
        """
        pass

    @abstractmethod
    def clone(self, reference: Any, other: Any) -> None:
        """
        Clones a database table (structure only or with data).

        Args:
            reference: The source table to clone from.
            other: The destination table to create.
        """
        pass

    @abstractmethod
    def minus(
        self,
        reference: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> int:
        """
        Returns the number of rows in reference that do not exist in other.

        Args:
            reference: The reference table name.
            other: The table to compare against.
            exclude_columns: Optional set of columns to exclude from comparison.

        Returns:
            int: The number of rows in reference that do not exist in other.
        """
        pass

    @abstractmethod
    def drop(self, identifier: str) -> None:
        """
        Physically removes a table or container from the database.

        Args:
            identifier: The name of the table or object to drop.
        """

    @abstractmethod
    def get_checksum(self, identifier: str, columns: list[str] | None = None) -> str:
        """
        Generates a unique fingerprint for the data in a table.

        Args:
            identifier: The table name.
            columns: Optional list of columns to include in the checksum.

        Returns:
            str: A string representing the checksum.
        """
        pass

    @protect_service(breaker)
    def exists(self, identifier: str) -> bool:
        """
        Checks for the existence of a database artifact.

        Args:
            identifier: The unique name of the artifact to check.

        Returns:
            bool: True if it exists, False otherwise.
        """
        return self.client.exists(identifier)
