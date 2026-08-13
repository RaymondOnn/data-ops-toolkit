import threading
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from libs.database.pool.base import ConnectionPool, LockPool


class DBClient(ABC):
    """
    Abstract base class for database clients.

    Provides a standardized interface for connection leasing, pooling,
    and data frame operations across various database engines.
    """

    type: str

    def __init__(self, **config: Any) -> None:
        """
        Initializes the database client.

        Args:
            **config: Driver-specific configuration parameters.

        Notes:
        - We initialize a threading Lock here because clients are often shared
        singletons within a process. This prevents race conditions during
        lazy connection initialization, especially in multi-threaded
        environments or when used alongside Ray.

        - We avoid initializing the pool in __init__ to prevent network
        round-trips during object instantiation. This ensures that creating
        a Client object is cheap and safe to perform on the Orchestrator.

        - We default to a LockPool (Size 1) to protect legacy drivers that
        are not thread-safe, ensuring system stability out-of-the-box.

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

        Notes:
        - We avoid initializing the pool in __init__ to prevent network
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

        Notes:
        - We default to a LockPool (Size 1) to protect legacy drivers that
        are not thread-safe, ensuring system stability out-of-the-box.
        """
        return LockPool(connector=self.connect)

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """
        Leases a connection from the pool.

        Yields:
            Any: A database connection object.

        Notes:
        - By using a context manager for leasing, we guarantee that connections
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
    def command(self, sql: str, params: Any = None) -> None:
        """Executes non-query SQL (DDL, INSERT, UPDATE, DELETE). No returns."""
        raise NotImplementedError()

    @abstractmethod
    def query(self, sql: str, params: Any = None) -> Generator[Any, None, None]:
        """
        Executes a SELECT query and yields data blocks.
        Yields PyArrow RecordBatches or raw row tuple sequences for memory safety.
        """
        raise NotImplementedError()

    @abstractmethod
    def copy(
        self,
        table: str,
        source_dir: str,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> None:
        """High-throughput file ingestion directly into target tables."""
        raise NotImplementedError()
