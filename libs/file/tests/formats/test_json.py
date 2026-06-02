import io
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from libs.file.formats.json import JSONHandler


class TestJSONHandler:
    """Unit tests for the JSONHandler."""

    @pytest.fixture
    def mock_fs(self):
        """Returns a MagicMock for fsspec.AbstractFileSystem."""
        mock = MagicMock(spec=AbstractFileSystem)
        mock._strip_protocol.side_effect = lambda x: x.replace("s3://", "")
        mock.unstrip_protocol.side_effect = lambda x: f"s3://{x}"
        return mock

    @pytest.fixture
    def handler(self, mock_fs):
        """Returns a JSONHandler instance with a mocked filesystem."""
        return JSONHandler(fs=mock_fs)

    def test_is_splittable(self, handler):
        """
        GIVEN a JSONHandler instance
        THEN the is_splittable property should return False
        WHEN accessed
        """
        assert handler.is_splittable is False

    def test_discover_json_variants(self, handler, mock_fs):
        """
        GIVEN a directory containing various JSON extensions
        THEN discover should return all .json, .jsonl, and .ndjson files
        WHEN discover is called
        """
        mock_fs.isfile.side_effect = [False, True, True, True]
        mock_fs.glob.return_value = [
            "bucket/data.json",
            "bucket/data.jsonl",
            "bucket/data.ndjson",
        ]
        path = "s3://bucket/"
        result = handler.discover(path)

        assert "s3://bucket/data.json" in result
        assert "s3://bucket/data.jsonl" in result
        assert "s3://bucket/data.ndjson" in result

    def test_read_trailing_comma_repair(self, handler, mock_fs):
        """
        GIVEN a malformed JSON file with a trailing comma: {"a": 1, }
        THEN the read method should repair it to: {"a": 1}
        WHEN read is called
        """
        # Malformed JSON with trailing commas in both object and array
        malformed = b'{"items": [1, 2, ], "meta": "test", }'
        mock_fs.open.return_value.__enter__.return_value = io.BytesIO(malformed)

        with patch.object(handler, "discover", return_value={"s3://bucket/bad.json"}):
            buffer = handler.read("s3://bucket/bad.json")
            repaired_content = buffer.getvalue().decode()

            assert repaired_content == '{"items": [1, 2], "meta": "test"}'

    @patch("polars.scan_ndjson")
    def test_to_df_ndjson_extension(self, mock_scan, handler, mock_fs):
        """
        GIVEN a file with a .jsonl extension
        THEN to_df should use the high-performance scan_ndjson path
        WHEN to_df is called
        """
        mock_fs.isfile.return_value = True
        path = "s3://bucket/stream.jsonl"

        handler.to_df(path)

        mock_scan.assert_called_once()
        # Verify it passed a list containing the path to scan_ndjson
        args, _ = mock_scan.call_args
        assert args[0] == [path]

    @patch("polars.scan_ndjson")
    def test_to_df_ndjson_peeking(self, mock_scan, handler, mock_fs):
        """
        GIVEN a file with a .json extension but NDJSON content (starts with '{')
        THEN to_df should detect the format via peeking and use scan_ndjson
        WHEN to_df is called
        """
        mock_fs.isfile.return_value = True
        path = "s3://bucket/hidden_ndjson.json"

        # Mock the peek check: first byte is '{'
        mock_fs.open.return_value.__enter__.return_value = io.BytesIO(b'{"id": 1}\n')

        handler.to_df(path)
        mock_scan.assert_called_once()

    @patch("polars.read_json")
    def test_to_df_standard_json(self, mock_read_json, handler, mock_fs):
        """
        GIVEN a standard JSON array file (starts with '[')
        THEN to_df should use the defensive read_json path with repairs
        WHEN to_df is called
        """
        mock_fs.isfile.return_value = True
        mock_fs.size.return_value = 100
        path = "s3://bucket/array.json"

        # Mock peek: starts with '['
        mock_fs.open.return_value.__enter__.return_value = io.BytesIO(b'[{"a": 1}]')
        # Mock return of read_json
        mock_read_json.return_value = pl.DataFrame({"a": [1]})

        df = handler.to_df(path)

        assert isinstance(df, pl.LazyFrame)
        mock_read_json.assert_called_once()

    @patch("polars.LazyFrame.sink_ndjson")
    def test_from_df_lazyframe(self, mock_sink, handler):
        """
        GIVEN a Polars LazyFrame
        THEN from_df should call sink_ndjson for streaming write
        WHEN from_df is called
        """
        lf = pl.LazyFrame({"a": [1]})
        handler.from_df(lf, "s3://bucket/output.json")
        mock_sink.assert_called_once_with("s3://bucket/output.json")

    @patch("polars.DataFrame.write_ndjson")
    def test_from_df_dataframe(self, mock_write, handler):
        """
        GIVEN a Polars DataFrame
        THEN from_df should call write_ndjson for direct write
        WHEN from_df is called
        """
        df = pl.DataFrame({"a": [1]})
        handler.from_df(df, "s3://bucket/output.json")
        mock_write.assert_called_once_with("s3://bucket/output.json")

    def test_write_bytes(self, handler, mock_fs):
        """
        GIVEN raw bytes data
        THEN write should open the file in binary mode and write the data
        WHEN write is called
        """
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__.return_value = mock_file

        data = b'{"raw": "data"}'
        handler.write(data, "s3://bucket/raw.json")

        mock_fs.open.assert_called_once_with("s3://bucket/raw.json", "wb")
        mock_file.write.assert_called_once_with(data)
