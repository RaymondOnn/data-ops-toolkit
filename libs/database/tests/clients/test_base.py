from collections.abc import Generator, Sequence
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
from libs.database.clients.base import DBClient


class MockDBClient(DBClient):
    """Concrete class to test DBClient logic."""

    @property
    def type(self) -> str:
        return "mock-db"

    def connect(self) -> MagicMock:
        return MagicMock()

    def _ping(self, conn: Any) -> None:
        pass

    def sql(self, query: str) -> list[Sequence[Any]]:
        return [("mock", 1)]

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        yield pl.DataFrame({"a": [1, 2]})

    def partition_load(self, table_name, num_workers=10, filter_sql=None):
        return {"SELECT * FROM table"}

    def get_schema(self, fq_table):
        return pl.DataFrame()

    def exists(self, fq_table):
        return True

    def copy_from_file(self, table, source_dir, file_ext="parquet", audit_values=None):
        pass


class TestDBClientBase:
    """Unit tests for the abstract DBClient core logic."""

    def test_lazy_pool_initialization(self):
        """
        GIVEN a DBClient instance
        THEN the pool should be initialized only when first accessed
        WHEN pool property is invoked
        """
        client = MockDBClient(user="test")
        assert client._pool is None

        pool = client.pool
        assert pool is not None
        assert client._pool is pool

    def test_get_connection_context_manager(self):
        """
        GIVEN a DBClient with a mocked pool
        THEN it should lease and yield a connection from the pool
        WHEN get_connection is used as a context manager
        """
        client = MockDBClient()
        mock_conn = MagicMock()

        with patch.object(client.pool, "lease") as mock_lease:
            mock_lease.return_value.__enter__.return_value = mock_conn

            with client.get_connection() as conn:
                assert conn == mock_conn

            mock_lease.assert_called_once()

    def test_fetch_lazy_concatenation(self):
        """
        GIVEN a fetch_df generator yielding multiple batches
        THEN fetch_lazy should return a single concatenated LazyFrame
        WHEN fetch_lazy is called
        """
        client = MockDBClient()

        def mock_batches(query):
            yield pl.DataFrame({"id": [1]})
            yield pl.DataFrame({"id": [2]})

        with patch.object(client, "fetch_df", side_effect=mock_batches):
            lf = client.fetch_lazy("SELECT *")
            assert isinstance(lf, pl.LazyFrame)

            # Materialize to verify content
            df = lf.collect()
            assert df.height == 2
            assert df["id"].to_list() == [1, 2]

    def test_reconnect_clears_pool(self):
        """
        GIVEN an active DBClient pool
        THEN reconnect should close all connections in the pool
        WHEN reconnect is invoked
        """
        client = MockDBClient()
        pool = client.pool

        with patch.object(pool, "close_all") as mock_close:
            client.reconnect()
            mock_close.assert_called_once()

    def test_type_property(self):
        """
        GIVEN a concrete MockDBClient
        THEN it should return the correct type identifier
        WHEN type property is accessed
        """
        client = MockDBClient()
        assert client.type == "mock-db"
