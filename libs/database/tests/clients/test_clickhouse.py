from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from libs.database.clients.clickhouse import ClickhouseClient, get_error_code
from libs.utils.exceptions import AuthFailure, HostUnreachable


class TestClickhouseClient:
    """Unit tests for the ClickhouseClient."""

    @pytest.fixture
    def client(self):
        """Returns a ClickhouseClient for testing."""
        return ClickhouseClient(host="localhost", user="test", password="pwd")

    def test_get_error_code(self):
        """
        GIVEN an exception message containing a ClickHouse code
        THEN return the integer code correctly
        WHEN get_error_code is called
        """
        msg = "DB::Exception: Some error. (Code: 192)"
        assert get_error_code(Exception(msg)) == 192
        assert get_error_code(Exception("No code here")) is None

    @patch("clickhouse_connect.get_client")
    def test_connect_auth_failure(self, mock_get_client, client):
        """
        GIVEN a ClickHouse authentication error (Code 192)
        THEN raise a platform-specific AuthFailure exception
        WHEN connect is invoked
        """
        mock_get_client.side_effect = DatabaseError("Access Denied (Code: 192)")

        with pytest.raises(AuthFailure):
            client.connect()

    @patch("clickhouse_connect.get_client")
    def test_connect_host_unreachable(self, mock_get_client, client):
        """
        GIVEN a connection refused error
        THEN raise a platform-specific HostUnreachable exception
        WHEN connect is invoked
        """
        mock_get_client.side_effect = DatabaseError("Connection refused")

        with pytest.raises(HostUnreachable):
            client.connect()

    def test_partition_load(self, client):
        """
        GIVEN a request for 3 workers
        THEN return 3 SQL queries using cityHash64
        WHEN partition_load is called
        """
        queries = client.partition_load("my_table", num_workers=3)
        assert len(queries) == 3
        assert "cityHash64(*)" in next(iter(queries))
        assert True

    def test_sql_execution(self, client):
        """
        GIVEN a mocked connection and result
        THEN return results as a list of tuples
        WHEN sql() is called
        """
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.result_rows = [(1, "a"), (2, "b")]
        mock_conn.query.return_value = mock_result

        with patch.object(client, "get_connection") as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_conn

            rows = client.sql("SELECT *")
            assert rows == [(1, "a"), (2, "b")]
            mock_conn.query.assert_called_once_with("SELECT *")

    def test_fetch_df_streaming(self, client):
        """
        GIVEN a generator yielding Pandas DataFrames from the driver
        THEN yield Polars DataFrames to the caller
        WHEN fetch_df is called
        """
        mock_conn = MagicMock()
        # Simulate a stream of 2 pandas batches
        mock_stream = [pd.DataFrame({"id": [1]}), pd.DataFrame({"id": [2]})]
        mock_conn.query_df_stream.return_value = mock_stream

        with patch.object(client, "get_connection") as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_conn

            results = list(client.fetch_df("SELECT *"))

            assert len(results) == 2
            assert isinstance(results[0], pl.DataFrame)
            assert results[0]["id"][0] == 1

    def test_exists_logic(self, client):
        """
        GIVEN a table that exists in ClickHouse
        THEN return True
        WHEN exists() is called
        """
        # EXISTS TABLE returns a row like (1,)
        with patch.object(client, "sql", return_value=[(1,)]):
            assert client.exists("mydb.my_table") is True

        with patch.object(client, "sql", return_value=[(0,)]):
            assert client.exists("missing") is False
