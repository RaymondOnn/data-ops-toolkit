import threading
from abc import ABC, abstractmethod
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import Any

import polars as pl
from libs.database.pool.base import ConnectionPool, LockPool


class DBClient(ABC):
    """
    Abstract base class for database clients.

    Provides a standardized interface for connection leasing, pooling,
    and data frame operations across various database engines.
    """

    def __init__(self, **config: Any) -> None:
        """
        Initializes the database client.

        Args:
            **config: Driver-specific configuration parameters.

        Decision: Process-Level Isolation.
        We initialize a threading Lock here because clients are often shared
        singletons within a process. This prevents race conditions during
        lazy connection initialization, especially in multi-threaded
        environments or when used alongside Ray.
        """
        self.config = config
        # Base lock to prevent concurrent access to process-level singletons
        self._lock = threading.Lock()
        self._pool: ConnectionPool | None = None

    @property
    def pool(self) -> ConnectionPool:
        """
        Lazy initialization of the pool strategy.

        Returns:
            ConnectionPool: The active connection pool instance.

        Decision: Lazy Binding.
        We avoid initializing the pool in __init__ to prevent network
        round-trips during object instantiation. This ensures that creating
        a Client object is cheap and safe to perform on the Orchestrator.
        """
        if self._pool is None:
            self._pool = self._init_pool()
        return self._pool

    def _init_pool(self) -> ConnectionPool:
        """
        Initializes the connection pool strategy.

        Returns:
            ConnectionPool: A fallback LockPool for lean execution.

        Decision: Safe Defaults.
        We default to a LockPool (Size 1) to protect legacy drivers that
        are not thread-safe, ensuring system stability out-of-the-box.
        """
        return LockPool(connector=self.connect)

    @property
    def type(self) -> str:
        """
        Returns a string identifier for the database type.

        Returns:
            str: Identifier (e.g., 'clickhouse', 'oracle').
        """
        raise NotImplementedError("Subclasses must implement this property")

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """
        Leases a connection from the pool.

        Yields:
            Any: A database connection object.

        Decision: Resource Guarding.
        By using a context manager for leasing, we guarantee that connections
        are returned to the pool even if a query fails, preventing 'Connection
        Leak' outages in long-running ingestion jobs.
        """
        with self.pool.lease() as conn:
            yield conn

    def close(self) -> None:
        """
        Closes all active connections in the pool and shuts down the client.
        """
        if self._pool:
            self._pool.close_all()

    @abstractmethod
    def connect(self) -> Any:
        """
        Establishes a raw connection to the database.

        Returns:
            Any: The established connection object.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def _ping(self, conn: Any) -> None:
        """
        Verifies connection health.

        Args:
            conn: The connection object to test.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def reconnect(self) -> None:
        """
        Resets the entire connection pool to clear broken pipes.
        """
        if self._pool:
            self._pool.close_all()
        # Connection will re-initialize lazily on next lease

    @abstractmethod
    def sql(self, query: str) -> list[Sequence[Any]]:
        """
        Executes a query and returns results as raw tuples.

        Args:
            query: The SQL query string.

        Returns:
            list[Sequence[Any]]: A list of row tuples.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """
        Executes a query and yields results as Polars DataFrames.

        Args:
            query: The SQL query string.

        Yields:
            pl.DataFrame: A batch of query results.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def fetch_lazy(self, query: str) -> pl.LazyFrame:
        """
        Executes a query and returns a concatenated Polars LazyFrame.

        Args:
            query: The SQL query string.

        Returns:
            pl.LazyFrame: The combined lazy representation of results.

        Decision: Memory Safety.
        We wrap the stream in a LazyFrame to allow Polars to perform
        predicate pushdown and projection pushdown, which is vital for
        staying under the 2GB RAM ceiling when handling 50M rows.
        """
        return pl.concat(self.fetch_df(query), how="vertical").lazy()

    @abstractmethod
    def partition_load(
        self,
        table_name: str,
        num_workers: int = 10,
        filter_condition: str | None = None,
    ) -> set[str]:
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

    @abstractmethod
    def get_schema(self, fq_table: str) -> pl.DataFrame:
        """
        Retrieves the physical schema from the database system tables.

        Args:
            fq_table: Fully qualified table name.

        Returns:
            pl.DataFrame: Metadata report including columns and types.
        """
        raise NotImplementedError()

    @abstractmethod
    def exists(self, fq_table: str) -> bool:
        """
        Checks for the existence of a database artifact.

        Args:
            fq_table: Fully qualified name of the artifact.

        Returns:
            bool: True if it exists, False otherwise.
        """
        raise NotImplementedError()

    @abstractmethod
    def copy_from_file(
        self,
        table: str,
        source_dir: str,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> None:
        """
        Performs a bulk load from files into the target table.

        Args:
            table: Destination table name.
            source_dir: Directory containing files to load.
            file_ext: Format of the source files.
            audit_values: Constants to inject during loading.
        """
        raise NotImplementedError("Subclasses must implement this method")
