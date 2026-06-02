import io
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from libs.file.mixins.data import FlatFileMixin


class MockDataClient(FlatFileMixin):
    """Concrete class to test the Data Ingestion Mixin."""

    def __init__(self, fs):
        self.fs = fs
        self.opts = {}
        # Mock the abstract requirement of the client
        self.resolve_path = MagicMock(side_effect=lambda x: x)


class TestFlatFileMixin:
    """Unit tests for the FlatFileMixin."""

    @pytest.fixture
    def mock_fs(self):
        """Returns a MagicMock for fsspec.AbstractFileSystem."""
        return MagicMock(spec=AbstractFileSystem)

    @pytest.fixture
    def client(self, mock_fs):
        """Returns a MockDataClient instance."""
        return MockDataClient(fs=mock_fs)

    def test_is_file_readable_success(self, client, mock_fs):
        """
        GIVEN a file that exists and has data
        THEN return True
        WHEN is_file_readable is called
        """
        mock_fs.exists.return_value = True
        mock_fs.size.return_value = 1024
        assert client.is_file_readable(mock_fs, "data.csv") is True

    def test_is_file_readable_zero_byte(self, client, mock_fs):
        """
        GIVEN an empty file (zero bytes)
        THEN return False and log an error
        WHEN is_file_readable is called
        """
        mock_fs.exists.return_value = True
        mock_fs.size.return_value = 0
        assert client.is_file_readable(mock_fs, "empty.csv") is False

    @patch("libs.file.mixins.data.fsspec.filesystem")
    def test_mount_archive_fs_zip(self, mock_fsspec, client, mock_fs):
        """
        GIVEN a path to a .zip archive
        THEN it should initialize a ZipFileSystem and return contained files
        WHEN get_reader_context is called
        """
        mock_zip_fs = MagicMock()
        mock_zip_fs.find.return_value = ["file1.csv", "file2.json"]
        mock_fsspec.return_value = mock_zip_fs

        fs, targets = client.get_reader_context("archive.zip")

        assert fs == mock_zip_fs
        assert "file1.csv" in targets
        mock_fsspec.assert_called_with("zip", fo="archive.zip", remote_options={})

    def test_partition_load_chunking(self, client, mock_fs):
        """
        GIVEN a list of files totaling over 1GB
        THEN it should split them into separate batches
        WHEN partition_load is called
        """
        # Mock 3 files, each 600MB
        files = ["a.csv", "b.csv", "c.csv"]
        mock_fs.isdir.return_value = True
        mock_fs.find.return_value = files
        mock_fs.size.side_effect = [600 * 1024**2, 600 * 1024**2, 600 * 1024**2]

        with patch.object(client, "get_reader_context", return_value=(mock_fs, files)):
            batches = client.partition_load("dir/")

            # Batch 1: a + b (1.2GB) -> actually a + b exceeds 1GB,
            # so batch 1 should just be [a]?
            # Logic: if current + size > limit: append batch.
            # So [a] (600MB), then b comes: 600+600 > 1024. Append [a].
            # New batch [b]. Then c comes: 600+600 > 1024. Append [b].
            # New batch [c].
            assert len(batches) == 3
            assert batches[0] == ["a.csv"]

    @patch("charset_normalizer.from_bytes")
    def test_get_encoded_stream(self, mock_norm, client, mock_fs):
        """
        GIVEN a text file with Latin-1 encoding
        THEN it should detect the encoding using a sample
        WHEN _get_encoded_stream is called
        """
        mock_res = MagicMock()
        mock_res.best.return_value.encoding = "latin-1"
        mock_norm.return_value = mock_res

        mock_fs.open.return_value.__enter__.return_value = io.BytesIO(b"data")

        _, encoding = client._get_encoded_stream(mock_fs, "test.txt")
        assert encoding == "latin-1"

    @patch("libs.file.mixins.data.FormatFactory.get_handler")
    def test_fetch_df_integration(self, mock_get_handler, client, mock_fs):
        """
        GIVEN a list of CSV files
        THEN it should iterate, detect encoding, and concat LazyFrames
        WHEN fetch_df is called
        """
        mock_fs.exists.return_value = True
        mock_fs.size.return_value = 100

        mock_handler = MagicMock()
        mock_handler.to_df.return_value = pl.LazyFrame({"id": [1]})
        mock_get_handler.return_value = mock_handler

        # Mock encoding detection to return utf-8
        with patch.object(client, "_get_encoded_stream", return_value=(None, "utf-8")):
            lf = client.fetch_df(["f1.csv", "f2.csv"])

            result = lf.collect()
            assert result.height == 2  # 1 from each file
            assert mock_handler.to_df.call_count == 2
