from collections.abc import Generator, Sequence
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pytest
from apps.ingestion.src.services.database.base import (
    DatabaseService,
    DatabaseSink,
    DatabaseSource,
)
from libs.database.clients.base import DBClient
from libs.resilience.circuit_breaker import CircuitOpen


# Mock DBClient for testing DatabaseService
class MockDBClient(DBClient):
    """A mock DBClient implementation for testing purposes."""

    @property
    def type(self) -> str:
        return "mock-db"

    def connect(self) -> MagicMock:
        return MagicMock()

    def _ping(self, conn: Any) -> None:
        pass

    def sql(self, query: str) -> list[Sequence[Any]]:
        return [("mock_result",)]

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        yield pl.DataFrame({"col": [1]})

    def partition_load(self, table_name, num_workers=10, filter_condition=None):
        return {
            f"SELECT * FROM {table_name} WHERE part = {i}" for i in range(num_workers)
        }

    def get_schema(self, fq_table):
        return pl.DataFrame()

    def exists(self, fq_table):
        return True

    def copy_from_file(self, table, source_dir, file_ext="parquet", audit_values=None):
        pass


class TestDatabaseService:
    """Unit tests for the DatabaseService base class."""

    @pytest.fixture
    def mock_db_client(self):
        """Returns a MagicMock for DBClient."""
        return MockDBClient()

    @pytest.fixture
    def db_service(self, mock_db_client):
        """Returns a DatabaseService instance with a mocked client."""

        # DatabaseService is abstract, so we need a concrete class to test it
        class ConcreteDatabaseService(DatabaseService):
            @property
            def client(self) -> DBClient:
                return mock_db_client

            def count_units(
                self, target: str, filter_condition: str | None = None
            ) -> int:
                return 100

        return ConcreteDatabaseService(name="test-db", host="localhost")

    def test_reset_client(self, db_service, mock_db_client):
        """
        GIVEN an initialized client in the service
        THEN the cached client should be cleared
        WHEN reset_client is called
        """
        # Access client once to ensure it's cached
        _ = db_service.client
        assert "client" in db_service.__dict__

        db_service.reset_client()
        assert "client" not in db_service.__dict__

    def test_connection_context_manager(self, db_service, mock_db_client):
        """
        GIVEN a DatabaseService
        THEN it should lease a connection from the underlying client's pool
        WHEN connection is used as a context manager
        """
        with db_service.connection() as conn:
            assert conn == mock_db_client.get_connection().__enter__.return_value
        mock_db_client.get_connection.assert_called_once()

    def test_fetch_df_delegation(self, db_service, mock_db_client):
        """
        GIVEN a query
        THEN fetch_df should delegate to the client's fetch_df
        WHEN fetch_df is called
        """
        results = list(db_service.fetch_df("SELECT *"))
        assert len(results) == 1
        assert isinstance(results[0], pl.DataFrame)
        mock_db_client.fetch_df.assert_called_once_with("SELECT *")

    def test_fetch_delegation(self, db_service, mock_db_client):
        """
        GIVEN a query
        THEN fetch should delegate to the client's sql method
        WHEN fetch is called
        """
        results = db_service.fetch("SELECT 1")
        assert results == [("mock_result",)]
        mock_db_client.sql.assert_called_once_with("SELECT 1")

    def test_execute_batch_delegation(self, db_service, mock_db_client):
        """
        GIVEN a client that supports execute_batch
        THEN execute_batch should delegate to the client's method
        WHEN execute_batch is called
        """
        mock_db_client.execute_batch = MagicMock()
        db_service.execute_batch("INSERT ...", [{"id": 1}])
        mock_db_client.execute_batch.assert_called_once_with("INSERT ...", [{"id": 1}])

    def test_execute_batch_not_implemented(self, db_service):
        """
        GIVEN a client that does not support execute_batch
        THEN execute_batch should raise NotImplementedError
        WHEN execute_batch is called
        """
        del db_service.client.execute_batch  # Simulate missing method
        with pytest.raises(
            NotImplementedError, match="does not support batch execution"
        ):
            db_service.execute_batch("INSERT ...", [{"id": 1}])


class TestDatabaseSource:
    """Unit tests for the DatabaseSource base class."""

    @pytest.fixture
    def db_source(self, mock_db_client):
        """Returns a DatabaseSource instance with a mocked client."""

        class ConcreteDatabaseSource(DatabaseSource):
            @property
            def client(self) -> DBClient:
                return mock_db_client

            def count_units(
                self, target: str, filter_condition: str | None = None
            ) -> int:
                return 100

        return ConcreteDatabaseSource(name="test-db-source", host="localhost")

    def test_parallelize_delegation(self, db_source, mock_db_client):
        """
        GIVEN a target and worker count
        THEN parallelize should delegate to the client's partition_load
        WHEN parallelize is called
        """
        units = db_source.parallelize("my_table", 5)
        assert units == {f"SELECT * FROM my_table WHERE part = {i}" for i in range(5)}
        mock_db_client.partition_load.assert_called_once_with("my_table", 5, None)

    def test_pull_concatenation(self, db_source, mock_db_client):
        """
        GIVEN a work unit (query)
        THEN pull should fetch dataframes and concatenate them
        WHEN pull is called
        """
        # Mock fetch_df to yield multiple batches
        mock_db_client.fetch_df.side_effect = [
            pl.DataFrame({"id": [1]}),
            pl.DataFrame({"id": [2]}),
        ]

        df = db_source.pull("SELECT * FROM my_table WHERE part = 0")
        assert isinstance(df, pl.DataFrame)
        assert df.height == 2
        assert df["id"].to_list() == [1, 2]
        mock_db_client.fetch_df.assert_called_once_with(
            "SELECT * FROM my_table WHERE part = 0"
        )


class TestDatabaseSink:
    """Unit tests for the DatabaseSink base class."""

    @pytest.fixture
    def db_sink(self, mock_db_client):
        """Returns a DatabaseSink instance with a mocked client."""

        class ConcreteDatabaseSink(DatabaseSink):
            @property
            def client(self) -> DBClient:
                return mock_db_client

            def count_units(
                self, target: str, filter_condition: str | None = None
            ) -> int:
                return 100

            def stage(
                self,
                source_dir,
                target_location,
                expected_count,
                file_ext="parquet",
                audit_values=None,
            ):
                return "stg_table", 100

            def promote(
                self,
                staging_location,
                target_location,
                partition_by,
                partition_val,
                expected_count,
            ):
                pass

            def is_equal(self, reference, other, exclude_columns=None):
                return True

            def clone(self, reference, other):
                pass

            def minus(self, reference, other, exclude_columns=None):
                return 0

            def drop(self, identifier):
                pass

            def get_checksum(self, identifier, columns=None):
                return "checksum_val"

        return ConcreteDatabaseSink(name="test-db-sink", host="localhost")

    def test_exists_delegation(self, db_sink, mock_db_client):
        """
        GIVEN an identifier
        THEN exists should delegate to the client's exists method
        WHEN exists is called
        """
        assert db_sink.exists("my_table") is True
        mock_db_client.exists.assert_called_once_with("my_table")

    def test_exists_circuit_breaker_tripped(self, db_sink, mock_db_client):
        """
        GIVEN the underlying client raises a CircuitOpen exception
        THEN exists should re-raise CircuitOpen
        WHEN exists is called
        """
        mock_db_client.exists.side_effect = CircuitOpen("Breaker open")
        with pytest.raises(CircuitOpen):
            db_sink.exists("my_table")
