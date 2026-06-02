import io
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from libs.file.formats.parquet import ParquetHandler


class TestParquetHandler:
    """Unit tests for the ParquetHandler."""

    @pytest.fixture
    def mock_fs(self):
        """Returns a MagicMock for fsspec.AbstractFileSystem."""
        mock = MagicMock(spec=AbstractFileSystem)
        mock._strip_protocol.side_effect = lambda x: x.replace("s3://", "")
        mock.unstrip_protocol.side_effect = lambda x: f"s3://{x}"
        return mock

    @pytest.fixture
    def handler(self, mock_fs):
        """Returns a ParquetHandler instance with a mocked filesystem."""
        return ParquetHandler(fs=mock_fs)

    def test_is_splittable(self, handler):
        """
        GIVEN a ParquetHandler instance
        THEN the is_splittable property should return True
        WHEN accessed
        """
        assert handler.is_splittable is True

    def test_discover_single_file(self, handler, mock_fs):
        """
        GIVEN a path to a single Parquet file
        THEN discover should return a set containing that file's path
        WHEN discover is called
        """
        mock_fs.isfile.return_value = True
        path = "s3://bucket/data.parquet"
        result = handler.discover(path)
        assert result == {path}
        mock_fs.isfile.assert_called_with("bucket/data.parquet")

    def test_discover_recursive(self, handler, mock_fs):
        """
        GIVEN a directory path
        THEN discover should search recursively for .parquet files
        WHEN discover is called
        """
        mock_fs.isfile.side_effect = [False, True, True]
        mock_fs.glob.return_value = ["bucket/a.parquet", "bucket/b.parquet"]
        path = "s3://bucket/"
        result = handler.discover(path)

        assert result == {"s3://bucket/a.parquet", "s3://bucket/b.parquet"}
        mock_fs.glob.assert_called_with("bucket/**/*.parquet")

    def test_read_binary_data(self, handler, mock_fs):
        """
        GIVEN a Parquet file path
        THEN read should return an io.BytesIO containing the raw bytes
        WHEN read is called
        """
        mock_data = b"parquet-magic-bytes"
        mock_fs.open.return_value.__enter__.return_value = io.BytesIO(mock_data)

        with patch.object(
            handler, "discover", return_value={"s3://bucket/test.parquet"}
        ):
            buffer = handler.read("s3://bucket/test.parquet")
            assert buffer.getvalue() == mock_data

    @patch("polars.scan_parquet")
    def test_to_df_utilizes_scan(self, mock_scan, handler, mock_fs):
        """
        GIVEN discovered Parquet paths
        THEN to_df should invoke pl.scan_parquet for high performance
        WHEN to_df is called
        """
        mock_fs.isfile.return_value = True
        path = "s3://bucket/data.parquet"
        mock_scan.return_value = pl.LazyFrame({"a": [1]})

        df = handler.to_df(path)

        assert isinstance(df, pl.LazyFrame)
        mock_scan.assert_called_once_with([path], storage_options={})

    @patch("polars.LazyFrame.sink_parquet")
    def test_from_df_lazyframe_sink(self, mock_sink, handler):
        """
        GIVEN a Polars LazyFrame
        THEN from_df should call sink_parquet for memory efficiency
        WHEN from_df is called
        """
        lf = pl.LazyFrame({"col": [1]})
        handler.from_df(lf, "s3://output.parquet")

        mock_sink.assert_called_once()
        args, kwargs = mock_sink.call_args
        assert args[0] == "s3://output.parquet"
        assert kwargs["compression"] == "snappy"

    @patch("polars.DataFrame.write_parquet")
    def test_from_df_dataframe_write(self, mock_write, handler):
        """
        GIVEN a Polars DataFrame
        THEN from_df should call write_parquet
        WHEN from_df is called
        """
        df = pl.DataFrame({"col": [1]})
        handler.from_df(df, "s3://output.parquet")

        mock_write.assert_called_once_with("s3://output.parquet", compression="snappy")

    def test_write_binary(self, handler, mock_fs):
        """
        GIVEN raw bytes
        THEN write should write them directly to the filesystem in binary mode
        WHEN write is called
        """
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__.return_value = mock_file

        data = b"raw-data"
        handler.write(data, "s3://output.bin")

        mock_fs.open.assert_called_once_with("s3://output.bin", "wb")
        mock_file.write.assert_called_once_with(data)
