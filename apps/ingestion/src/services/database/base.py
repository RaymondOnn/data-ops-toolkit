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
    """
    Intermediate layer for all SQL-based sources.
    """

    def __init__(self, name: str, **config: Any):
        super().__init__(name, **config)
        self.client: DBClient = self._init_client(**config)

    @abstractmethod
    def _init_client(self, **config: Any) -> Any:
        """Subclasses must initialize their specific DB client."""
        pass

    @contextlib.contextmanager
    def connection(self) -> Generator[Any, None, None]:
        """Exposes the underlying client's pooled connection lease."""
        with self.client.get_connection() as conn:
            yield conn

    @protect_service(breaker)
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """
        Executes a query and expects dict-like rows.
        """
        return self.client.fetch_df(query)

    @protect_service(breaker)
    def fetch(self, query: str) -> list[Sequence[Any]]:
        """
        Executes a query and expects dict-like rows.
        """
        return self.client.sql(query)

    @protect_service(breaker)
    def execute_batch(self, query: str, data: list[dict[str, Any]]) -> None:
        """
        Executes a parameterized query with a batch of data.
        """
        if hasattr(self.client, "execute_batch"):
            self.client.execute_batch(query, data)  # type: ignore
        else:
            raise NotImplementedError(
                "Database client does not support batch execution."
            )


class DatabaseSource(DatabaseService, Source):
    def get_work_units(
        self, target: str, num_workers: int, filter_sql: str | None = None
    ) -> set[str]:
        # All DBs use the client's load strategy (e.g., ORA_HASH, ctid)
        return self.client.get_load_strategy(target, num_workers, filter_sql)

    @protect_service(breaker)
    def fetch_data(self, unit: str) -> pl.DataFrame:
        """
        Implementation of Source.fetch_data for Databases.
        Consumes the generator from the client and returns a single DataFrame.
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
        """Phase 1: Returns the name of the temporary staging table and rows loaded."""
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
        """Phase 2: Moves data to production (Swap/Merge/Append)."""
        pass

    @abstractmethod
    def is_equal(
        self,
        reference: Path,
        other: Path,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        pass

    @abstractmethod
    def clone(self, reference: str, other: str) -> None:
        """Clone a table to a new table."""

    @protect_service(breaker)
    def exists(self, identifier: str) -> bool:
        """Implementation of Sink.exists for Databases."""
        return self.client.exists(identifier)
