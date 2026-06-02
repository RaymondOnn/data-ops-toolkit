from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from libs.file.mixins.archive import StandardArchiveMixin


class MockArchiveClient(StandardArchiveMixin):
    """Concrete class to test the Archive Mixin."""

    def __init__(self, fs, url):
        self.fs = fs
        self.url = url
        self.opts = {}


class TestStandardArchiveMixin:
    """Unit tests for the StandardArchiveMixin."""

    @pytest.fixture
    def mock_fs(self):
        """Returns a MagicMock for fsspec.AbstractFileSystem."""
        return MagicMock(spec=AbstractFileSystem)

    @pytest.fixture
    def client(self, mock_fs):
        """Returns a MockArchiveClient initialized with a mocked filesystem."""
        return MockArchiveClient(fs=mock_fs, url="s3://bucket")

    def test_archive_snapshot_file_copy(self, client, mock_fs):
        """
        GIVEN a local file path
        THEN it should create the destination directory and copy the file
        WHEN archive_snapshot is called
        """
        logical_date = datetime(2024, 1, 1)
        result_path = client.archive_snapshot(
            data="/tmp/source.csv",
            job_id="job1",
            dataset_name="users",
            category="source",
            logical_date=logical_date,
        )

        expected_dir = "s3://bucket/archive/job1/users/2024/01/01/source"
        expected_path = f"{expected_dir}/source.csv"

        assert result_path == expected_path
        mock_fs.makedirs.assert_called_once_with(expected_dir, exist_ok=True)
        mock_fs.cp.assert_called_once_with("/tmp/source.csv", expected_path)

    @patch("polars.LazyFrame.sink_parquet")
    def test_archive_snapshot_lazyframe(self, mock_sink, client, mock_fs):
        """
        GIVEN a Polars LazyFrame
        THEN it should invoke sink_parquet to the structured archive path
        WHEN archive_snapshot is called
        """
        lf = pl.LazyFrame({"a": [1]})
        logical_date = datetime(2024, 5, 20)

        result_path = client.archive_snapshot(
            data=lf,
            job_id="etl_job",
            dataset_name="orders",
            category="bronze",
            logical_date=logical_date,
        )

        expected_path = (
            "s3://bucket/archive/etl_job/orders/2024/05/20/bronze/orders_bronze.parquet"
        )
        assert result_path == expected_path
        mock_sink.assert_called_once_with(expected_path)

    def test_restore_from_archive_success(self, client, mock_fs):
        """
        GIVEN a valid archive directory containing files
        THEN return the first file path found in that directory
        WHEN restore_from_archive is called
        """
        logical_date = datetime(2024, 2, 10)
        mock_fs.exists.return_value = True
        mock_fs.ls.return_value = ["s3://bucket/archive/j/d/2024/02/10/s/file.csv"]

        path = client.restore_from_archive("j", "d", logical_date)

        assert "file.csv" in path
        mock_fs.exists.assert_called_once()

    def test_restore_from_archive_missing(self, client, mock_fs):
        """
        GIVEN a non-existent archive directory
        THEN raise a FileNotFoundError
        WHEN restore_from_archive is called
        """
        mock_fs.exists.return_value = False
        with pytest.raises(FileNotFoundError, match="No archive"):
            client.restore_from_archive("job", "ds", datetime.now())

    def test_apply_retention_policy_dry_run(self, client, mock_fs):
        """
        GIVEN a list of folders, some older than the retention threshold
        THEN it should log the deletion but not call fs.rm
        WHEN apply_retention_policy is called with dry_run=True
        """
        # Mock directory structure for traversal
        mock_fs.exists.return_value = True
        mock_fs.ls.side_effect = [
            ["s3://bucket/archive/j/d/2020"],  # Years
            ["s3://bucket/archive/j/d/2020/01"],  # Months
            ["s3://bucket/archive/j/d/2020/01/01/"],  # Days
        ]

        # Cutoff is roughly now, so 2020 is definitely expired
        client.apply_retention_policy("j", "d", days=30, dry_run=True)

        mock_fs.rm.assert_not_called()

    def test_apply_retention_policy_execution(self, client, mock_fs):
        """
        GIVEN expired folders and dry_run=False
        THEN it should call fs.rm recursively on the specific day directory
        WHEN apply_retention_policy is called
        """
        mock_fs.exists.return_value = True
        mock_fs.ls.side_effect = [["2020"], ["01"], ["01/"]]

        client.apply_retention_policy("j", "d", days=1, dry_run=False)
        mock_fs.rm.assert_called_with("01/", recursive=True)
