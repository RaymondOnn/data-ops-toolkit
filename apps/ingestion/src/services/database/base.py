from abc import abstractmethod
from collections.abc import Generator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import structlog
from src.services.base import Service, SinkMixin, SourceMixin
from src.services.registry import protect_service

from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreaker

if TYPE_CHECKING:
    from libs.clients.database.base import DBClient


LOG = structlog.get_logger(__name__)

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

    @protect_service(breaker)
    def sql(self, query: str) -> list[Sequence[Any]]:
        """
        Executes a standard SQL query and returns a list of rows.
        """
        return self.client.sql(query)

    @protect_service(breaker)
    def fetch(self, query: str) -> list[dict[str, Any]]:
        """
        Executes a query and expects dict-like rows.
        """
        if hasattr(self.client, "fetch"):
            return self.client.fetch(query)  # type: ignore
        return self.client.sql(query)  # type: ignore

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

    @protect_service(breaker)
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        # Centralized protected fetch for all DB types
        return self.client.fetch_df(query)


class DatabaseSource(DatabaseService, SourceMixin):
    def get_work_units(
        self, target: str, num_partitions: int, filter_sql: str | None = None
    ) -> list[str]:
        # All DBs use the client's load strategy (e.g., ORA_HASH, ctid)
        return self.client.get_load_strategy(target, num_partitions, filter_sql)


class DatabaseSink(DatabaseService, SinkMixin):
    @abstractmethod
    def stage_data(self, source_dir: Path, target_table: str) -> tuple[str, int]:
        """Phase 1: Returns the name of the temporary staging table and rows loaded."""
        pass

    @abstractmethod
    def promote_data(
        self,
        staging_table: str,
        target_table: str,
        partition_col: str,
        partition_val: str,
    ) -> None:
        """Phase 2: Moves data to production (Swap/Merge/Append)."""
        pass

    @abstractmethod
    def is_equal(
        self,
        reference: Path,
        other: Path,
        exclude_columns: list[str] | None = None,
    ) -> bool:
        pass

    @abstractmethod
    def clone(self, reference: str, other: str) -> None:
        """Clone a table to a new table."""
        pass
