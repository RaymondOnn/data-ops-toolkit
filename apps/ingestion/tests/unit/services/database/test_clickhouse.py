from unittest.mock import MagicMock, PropertyMock, patch

import polars as pl
import pytest
from apps.ingestion.src.services.database.clickhouse import ClickHouseService
from libs.auth.secret import Secret
from libs.database.clients.clickhouse import ClickhouseClient


class TestClickHouseService:
    """Unit tests for the ClickHouseService."""

    @pytest.fixture
    def mock_clickhouse_client(self):
        """Returns a MagicMock for the underlying ClickhouseClient."""
        client = MagicMock(spec=ClickhouseClient)
        client.sql.return_value = []  # Default for sql calls
        client.exists.return_value = True  # Default for exists calls
        client.fetch_df.return_value = [pl.DataFrame({"a": [1]})]
        return client

    @pytest.fixture
    def ch_service(self, mock_clickhouse_client):
        """
        Returns a ClickHouseService instance with its client property
        patched to use mock_clickhouse_client.
        """
        service = ClickHouseService(
            name="test_clickhouse",
            host="localhost",
            database="default",
            user="test_user",
            password="test_password",
        )
        # Patch the cached_property 'client' to return our mock
        with patch.object(
            ClickHouseService, "client", new_callable=PropertyMock
        ) as mock_prop:
            mock_prop.return_value = mock_clickhouse_client
            yield service

    def test_client_property_password_resolution(self, mock_clickhouse_client):
        """
        GIVEN a config with a Secret object for password
        THEN the client property should resolve the secret and pass plaintext
        WHEN client is accessed
        """
        mock_secret = MagicMock(spec=Secret)
        mock_secret.resolve.return_value = "resolved_secret"
        service = ClickHouseService(
            name="test_ch_secret",
            host="localhost",
            user="test",
            password=mock_secret,
        )
        # Temporarily patch the actual ClickhouseClient constructor
        with patch(
            "apps.ingestion.src.services.database.clickhouse.ClickhouseClient",
            return_value=mock_clickhouse_client,
        ) as mock_ch_client_cls:
            _ = service.client
            mock_ch_client_cls.assert_called_once_with(
                host="localhost",
                port=8123,
                user="test",
                password="resolved_secret",
                database="default",
            )
            mock_secret.resolve.assert_called_once_with(sanitize=True)

    def test_close_client(self, ch_service, mock_clickhouse_client):
        """
        GIVEN an initialized ClickHouseService
        THEN close() should call client.close() and clear the cached property
        WHEN close() is called
        """
        # Access client once to ensure it's cached
        _ = ch_service.client
        assert "client" in ch_service.__dict__

        ch_service.close()

        mock_clickhouse_client.close.assert_called_once()
        assert "client" not in ch_service.__dict__

    def test_get_total_count_delegation(self, ch_service, mock_clickhouse_client):
        """
        GIVEN a target table and filter condition
        THEN get_total_count should delegate to get_row_count
        WHEN get_total_count is called
        """
        with patch.object(ch_service, "get_row_count", return_value=500) as mock_grc:
            count = ch_service.get_total_count("my_table", "col > 10")
            assert count == 500
            mock_grc.assert_called_once_with("my_table", "col > 10")

    @patch("apps.ingestion.src.services.database.clickhouse.get_current_timestamp")
    def test_stage_data_success(
        self, mock_get_ts, ch_service, mock_clickhouse_client, tmp_path
    ):
        """
        GIVEN a source directory with files, a target table, and expected count
        THEN it should create a staging table, copy files, and return staging info
        WHEN stage_data is called
        """
        mock_get_ts.return_value = MagicMock(strftime=lambda x: "20240101120000")
        source_dir = tmp_path / "data"
        source_dir.mkdir()
        (source_dir / "file1.parquet").touch()
        (source_dir / "file2.parquet").touch()

        mock_clickhouse_client.sql.return_value = []
        with patch.object(ch_service, "get_row_count", return_value=200) as mock_grc:
            staging_table, rows_staged = ch_service.stage_data(
                source_dir, "mydb.target_table", 200
            )

            assert "stg_target_table_20240101120000" in staging_table
            assert rows_staged == 200
            mock_clickhouse_client.sql.assert_any_call(
                f"CREATE OR REPLACE TABLE {staging_table} "
                "ENGINE = MergeTree() ORDER BY tuple() AS mydb.target_table"
            )
            mock_clickhouse_client.copy_from_file.assert_called_once()
            mock_grc.assert_called_once_with(staging_table)

    @patch("apps.ingestion.src.services.database.clickhouse.get_current_timestamp")
    def test_stage_data_row_count_mismatch(
        self, mock_get_ts, ch_service, mock_clickhouse_client, tmp_path
    ):
        """
        GIVEN a row count mismatch after staging
        THEN it should raise ValueError and drop the staging table
        WHEN stage_data is called
        """
        mock_get_ts.return_value = MagicMock(strftime=lambda x: "20240101120000")
        source_dir = tmp_path / "data"
        source_dir.mkdir()
        (source_dir / "file1.parquet").touch()

        with (
            patch.object(ch_service, "get_row_count", return_value=100),
            pytest.raises(ValueError, match="Row count mismatch"),
        ):
            ch_service.stage_data(source_dir, "mydb.target_table", 200)

        # Verify cleanup
        mock_clickhouse_client.sql.assert_any_call(
            "DROP TABLE IF EXISTS stg_target_table_20240101120000"
        )

    def test_promote_data_success(self, ch_service, mock_clickhouse_client):
        """
        GIVEN a staging table, target table, partition info, and expected count
        THEN it should perform schema audit, delete existing partition, insert from
             staging, and drop staging table
        WHEN promote_data is called
        """
        mock_clickhouse_client.sql.side_effect = [
            # DESCRIBE TABLE target_table
            [("col1", "Int32"), ("col2", "String")],
            # DESCRIBE TABLE staging_table
            [("col1", "Int32"), ("col2", "String")],
            # DELETE FROM target_table
            [],
            # INSERT INTO target_table
            [],
            # DROP TABLE staging_table
            [],
        ]
        with patch.object(ch_service, "get_row_count", return_value=100) as mock_grc:
            ch_service.promote_data(
                "stg_table", "target_table", "dt", "2024-01-01", 100
            )
            mock_grc.assert_called_once()
            mock_clickhouse_client.sql.assert_any_call("DROP TABLE IF EXISTS stg_table")

    def test_promote_data_schema_mismatch(self, ch_service, mock_clickhouse_client):
        """
        GIVEN a schema mismatch between staging and target
        THEN it should raise ValueError
        WHEN promote_data is called
        """
        mock_clickhouse_client.sql.side_effect = [
            # DESCRIBE TABLE target_table
            [("col1", "Int32")],
            # DESCRIBE TABLE staging_table (missing col1)
            [("col2", "String")],
        ]
        with pytest.raises(ValueError, match="Schema mismatch"):
            ch_service.promote_data("stg_table", "target_table", "dt", "2024-01-01", 10)

    def test_is_equal_row_count_mismatch(self, ch_service):
        """
        GIVEN tables with different row counts
        THEN is_equal should return False
        WHEN is_equal is called
        """
        with patch.object(ch_service, "get_row_count", side_effect=[100, 99]):
            assert ch_service.is_equal("ref", "other") is False

    def test_is_equal_checksum_match(self, ch_service):
        """
        GIVEN tables with same checksums
        THEN is_equal should return True
        WHEN is_equal is called
        """
        with (
            patch.object(ch_service, "get_row_count", return_value=100),
            patch.object(ch_service, "get_checksum", return_value="abc"),
        ):
            assert ch_service.is_equal("ref", "other") is True

    def test_minus_logic(self, ch_service, mock_clickhouse_client):
        """
        GIVEN two tables
        THEN minus should execute the EXCEPT query and return the count
        WHEN minus is called
        """
        mock_clickhouse_client.fetch.side_effect = [
            # DESCRIBE TABLE reference
            [("col1",), ("col2",)],
            # DESCRIBE TABLE other
            [("col1",), ("col2",)],
        ]
        mock_clickhouse_client.sql.return_value = [(5,)]  # 5 rows in minus

        count = ch_service.minus("ref_table", "other_table")
        assert count == 5
        mock_clickhouse_client.sql.assert_called_with("""
            SELECT count() FROM (
                SELECT col1, col2 FROM ref_table
                EXCEPT
                SELECT col1, col2 FROM other_table
            )
        """)

    def test_clone_table(self, ch_service, mock_clickhouse_client):
        """
        GIVEN reference and other table names
        THEN clone should execute CREATE TABLE ... AS SELECT * FROM ... WHERE 1=0
        WHEN clone is called
        """
        ch_service.clone("source_table", "new_table")
        mock_clickhouse_client.sql.assert_called_once_with("""
            CREATE TABLE IF NOT EXISTS new_table
            ENGINE = MergeTree() AS
                SELECT * FROM source_table
                WHERE 1 = 0
        """)

    def test_get_checksum_logic(self, ch_service, mock_clickhouse_client):
        """
        GIVEN a table
        THEN get_checksum should execute the groupBitXor(cityHash64(...)) query
        WHEN get_checksum is called
        """
        mock_clickhouse_client.sql.return_value = [("abc",)]
        checksum = ch_service.get_checksum("my_table")
        assert checksum == "abc"
        mock_clickhouse_client.sql.assert_called_once_with(
            "SELECT hex(groupBitXor(cityHash64(*))) FROM my_table"
        )

    def test_drop_table(self, ch_service, mock_clickhouse_client):
        """
        GIVEN a table identifier
        THEN drop should execute DROP TABLE IF EXISTS ...
        WHEN drop is called
        """
        ch_service.drop("old_table")
        mock_clickhouse_client.sql.assert_called_once_with(
            "DROP TABLE IF EXISTS old_table"
        )

    def test_get_row_count_with_filter(self, ch_service, mock_clickhouse_client):
        """
        GIVEN a target and filter condition
        THEN get_row_count should construct and execute the correct COUNT(*) query
        WHEN get_row_count is called
        """
        mock_clickhouse_client.sql.return_value = [(123,)]
        count = ch_service.get_row_count("my_table", "dt = '2024-01-01'")
        assert count == 123
        mock_clickhouse_client.sql.assert_called_once_with(
            "SELECT COUNT(*) FROM my_table WHERE dt = '2024-01-01'"
        )

    def test_fetch_delegation(self, ch_service, mock_clickhouse_client):
        """
        GIVEN a query
        THEN fetch should delegate to client.sql
        WHEN fetch is called
        """
        ch_service.fetch("SELECT 1")
        mock_clickhouse_client.sql.assert_called_once_with("SELECT 1")
