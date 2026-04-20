import contextlib
import threading
from abc import ABC, abstractmethod
from collections.abc import Generator, Sequence
from typing import Any

import polars as pl

from ..pool.base import ConnectionPool, LockPool


class DBClient(ABC):
    def __init__(self, **config: Any) -> None:
        self.config = config
        # Base lock to prevent concurrent access to process-level singletons
        self._lock = threading.Lock()
        self._pool: ConnectionPool | None = None

    @property
    def pool(self) -> ConnectionPool:
        """Lazy initialization of the pool strategy."""
        if self._pool is None:
            self._pool = self._init_pool()
        return self._pool

    def _init_pool(self) -> ConnectionPool:
        """Default fallback to the LockPool (Lean Mode)."""
        return LockPool(connector=self.connect)

    @property
    def type(self) -> str:
        """Returns a string identifier for the database type (e.g., 'clickhouse', 'oracle')."""
        raise NotImplementedError("Subclasses must implement this property")

    @contextlib.contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """
        Standardized context manager for leasing connections.
        Subclasses can override this to implement pooling.
        """
        with self.pool.lease() as conn:
            yield conn

    @abstractmethod
    def connect(self) -> Any:
        """Specific driver logic to establish self._connection."""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def _ping(self, conn: Any) -> None:
        raise NotImplementedError("Subclasses must implement this method")

    def reconnect(self) -> None:
        """
        Ensures that a broken pipe during a folder-load
        resets the session entirely.
        """
        if self._pool:
            self._pool.close_all()
        # Connection will re-initialize lazily on next lease

    @abstractmethod
    def sql(self, query: str) -> list[Sequence[Any]]:
        """Yields DataFrames."""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """Yields DataFrames."""
        raise NotImplementedError("Subclasses must implement this method")

    def fetch_lazy(self, query: str) -> pl.LazyFrame:
        """
        Default implementation that wraps the existing fetch_df generator
        into a Polars LazyFrame.
        """
        return pl.concat(self.fetch_df(query), how="vertical").lazy()

    @abstractmethod
    def get_load_strategy(
        self,
        table_name: str,
        num_workers: int = 10,
        filter_sql: str | None = None,
    ) -> set[str]:
        """
        Convert a query into multiple "partition" queries
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def get_schema(self, fq_table: str) -> pl.DataFrame:
        """Returns a physical schema report from the DB system tables."""
        raise NotImplementedError()

    @abstractmethod
    def exists(self, identifier: str) -> bool:
        """Checks if a table or artifact exists."""
        raise NotImplementedError()

    @abstractmethod
    def copy_from_file(
        self, table: str, source_dir: str, file_ext: str = "parquet"
    ) -> None:
        """Default file copy method, can be overridden by databases with native support."""
        raise NotImplementedError("Subclasses must implement this method")
