import io
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from libs.file.formats.csv import CSVHandler


class TestCSVHandler:
    """Unit tests for the CSVHandler."""

    @pytest.fixture
    def mock_fs(self):
        """Returns a MagicMock for fsspec.AbstractFileSystem."""
        mock = MagicMock(spec=AbstractFileSystem)
        mock._strip_protocol.side_effect = lambda x: x.replace("s3://", "")
        mock.unstrip_protocol.side_effect = lambda x: f"s3://{x}"
        return mock

    @pytest.fixture
    def handler(self, mock_fs):
        """Returns a CSVHandler instance with a mocked filesystem."""
        return CSVHandler(fs=mock_fs)

    def test_is_splittable(self, handler):
        """
        GIVEN a CSVHandler instance
        THEN the is_splittable property should return True
        WHEN accessed
        """
        assert handler.is_splittable is True

    def test_discover_single_file(self, handler, mock_fs):
        """
        GIVEN a path to a single CSV file
        THEN discover should return a set containing that file's path
        WHEN discover is called
        """
        mock_fs.isfile.return_value = True
        path = "s3://bucket/data.csv"
        result = handler.discover(path)
        assert result == {path}
        mock_fs.isfile.assert_called_with("bucket/data.csv")

    def test_discover_glob_pattern(self, handler, mock_fs):
        """
        GIVEN a directory path with a glob pattern
        THEN discover should return all matching files
        WHEN discover is called
        """
        mock_fs.isfile.side_effect = [True, True]
        mock_fs.glob.return_value = ["bucket/dir/file1.csv", "bucket/dir/file2.txt"]
        path = "s3://bucket/dir"
        result = handler.discover(path, pattern="*.csv")
        assert result == {"s3://bucket/dir/file1.csv"}
        mock_fs.glob.assert_called_with("bucket/dir/*.csv")

    def test_discover_no_files(self, handler, mock_fs):
        """
        GIVEN a path with no matching files
        THEN discover should return an empty set
        WHEN discover is called
        """
        mock_fs.isfile.return_value = False
        mock_fs.glob.return_value = []
        path = "s3://bucket/empty_dir"
        result = handler.discover(path)
        assert result == set()

    def test_read_strips_bom(self, handler, mock_fs):
        """
        GIVEN a CSV file with a BOM
        THEN the read method should strip the BOM
        WHEN read is called
        """
        mock_fs.open.return_value.__enter__.return_value = io.BytesIO(
            b"\xef\xbb\xbfheader,value\n1,2"
        )

        with patch.object(handler, "discover", return_value={"s3://bucket/bom.csv"}):
            buffer = handler.read("s3://bucket/bom.csv")
            assert buffer.getvalue() == b"header,value\n1,2"

    def test_read_encoding_conversion(self, handler, mock_fs):
        """
        GIVEN a CSV file with a non-UTF-8 encoding
        THEN the read method should convert it to UTF-8
        WHEN read is called
        """
        mock_fs.discover.return_value = {"s3://bucket/latin1.csv"}
        mock_fs.open.return_value.__enter__.return_value = io.BytesIO(
            "héader,valué\n1,2".encode("latin-1")
        )

        buffer = handler.read("s3://bucket/latin1.csv", encoding="latin-1")
        assert buffer.getvalue() == "héader,valué\n1,2".encode()

    @patch("polars.scan_csv")
    @patch("libs.file.formats.csv.CSVHandler.discover")
    def test_to_df_scan_csv_large_file(
        self, mock_discover, mock_scan_csv, handler, mock_fs
    ):
        """
        GIVEN a large CSV file (over 2GB)
        THEN to_df should use pl.scan_csv for performance
        WHEN to_df is called
        """
        mock_discover.return_value = {"s3://bucket/large.csv"}
        mock_fs.size.return_value = 3 * 1024**3  # 3GB
        mock_scan_csv.return_value = pl.LazyFrame({"a": [1]})

        df = handler.to_df("s3://bucket/large.csv")
        assert isinstance(df, pl.LazyFrame)
        mock_scan_csv.assert_called_once()
        mock_scan_csv.assert_called_with(
            "s3://bucket/large.csv", storage_options={}, encoding="utf-8"
        )

    @patch("polars.read_csv")
    @patch("libs.file.formats.csv.CSVHandler.discover")
    @patch("libs.file.formats.csv.CSVHandler.read")
    def test_to_df_read_csv_small_file(
        self, mock_read, mock_discover, mock_read_csv, handler, mock_fs
    ):
        """
        GIVEN a small CSV file (under 2GB)
        THEN to_df should use pl.read_csv (via the read method)
        WHEN to_df is called
        """
        mock_discover.return_value = {"s3://bucket/small.csv"}
        mock_fs.size.return_value = 1 * 1024**3  # 1GB
        mock_read.return_value = io.BytesIO(b"col1,col2\n1,2")
        mock_read_csv.return_value = pl.DataFrame({"col1": [1], "col2": [2]})

        df = handler.to_df("s3://bucket/small.csv")
        assert isinstance(df, pl.LazyFrame)
        mock_read.assert_called_once_with("s3://bucket/small.csv")
        mock_read_csv.assert_called_once()

    @patch("polars.LazyFrame.sink_csv")
    def test_from_df_lazyframe(self, mock_sink_csv, handler):
        """
        GIVEN a Polars LazyFrame
        THEN from_df should call sink_csv for streaming write
        WHEN from_df is called
        """
        lf = pl.LazyFrame({"a": [1]})
        handler.from_df(lf, "s3://bucket/output.csv")
        mock_sink_csv.assert_called_once_with("s3://bucket/output.csv")

    @patch("polars.DataFrame.write_csv")
    def test_from_df_dataframe(self, mock_write_csv, handler):
        """
        GIVEN a Polars DataFrame
        THEN from_df should call write_csv for direct write
        WHEN from_df is called
        """
        df = pl.DataFrame({"a": [1]})
        handler.from_df(df, "s3://bucket/output.csv")
        mock_write_csv.assert_called_once_with("s3://bucket/output.csv")

    def test_write_bytes(self, handler, mock_fs):
        """
        GIVEN raw bytes data
        THEN write should open the file in binary mode and write the data
        WHEN write is called
        """
        mock_file = MagicMock(spec=io.BytesIO)
        mock_fs.open.return_value.__enter__.return_value = mock_file

        data = b"test data"
        handler.write(data, "s3://bucket/raw.bin")

        mock_fs.open.assert_called_once_with("s3://bucket/raw.bin", "wb")
        mock_file.write.assert_called_once_with(data)
