import io
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from libs.file.formats.xml import XMLHandler


class TestXMLHandler:
    """Unit tests for the XMLHandler."""

    @pytest.fixture
    def mock_fs(self):
        """Returns a MagicMock for fsspec.AbstractFileSystem."""
        mock = MagicMock(spec=AbstractFileSystem)
        mock._strip_protocol.side_effect = lambda x: x.replace("s3://", "")
        mock.unstrip_protocol.side_effect = lambda x: f"s3://{x}"
        return mock

    @pytest.fixture
    def handler(self, mock_fs):
        """Returns an XMLHandler instance with a mocked filesystem."""
        return XMLHandler(fs=mock_fs)

    def test_discover_xml_files(self, handler, mock_fs):
        """
        GIVEN a directory path
        THEN discover should return all .xml files
        WHEN discover is called
        """
        mock_fs.isfile.side_effect = [False, True, True]
        mock_fs.glob.return_value = ["bucket/a.xml", "bucket/b.xml"]
        path = "s3://bucket/"
        result = handler.discover(path)

        assert result == {"s3://bucket/a.xml", "s3://bucket/b.xml"}
        mock_fs.glob.assert_called_with("bucket/**/*.xml")

    def test_sanitize_removes_illegal_chars(self, handler, mock_fs):
        """
        GIVEN an XML string containing a null byte and other control chars
        THEN _sanitize should remove them while preserving valid XML tags
        WHEN _sanitize is called
        """
        # \x00 is illegal, \n and \t are legal
        raw_data = b"<root>\x00<item>Valid\nData</item></root>"
        mock_fs.open.return_value.__enter__.return_value = io.BytesIO(raw_data)

        clean_bytes = handler._sanitize("s3://test.xml")
        assert b"\x00" not in clean_bytes
        assert b"<root><item>Valid\nData</item></root>" in clean_bytes

    def test_read_combines_sanitized_files(self, handler, mock_fs):
        """
        GIVEN multiple discovered XML files
        THEN read should return a single buffer with combined sanitized content
        WHEN read is called
        """
        mock_fs.open.return_value.__enter__.side_effect = [
            io.BytesIO(b"<tag1/>"),
            io.BytesIO(b"<tag2/>"),
        ]

        with patch.object(
            handler, "discover", return_value={"s3://a.xml", "s3://b.xml"}
        ):
            buffer = handler.read("s3://bucket/")
            assert buffer.getvalue() == b"<tag1/><tag2/>"

    def test_to_df_bridge(self, handler, mock_fs):
        """
        GIVEN a simple XML structure
        THEN to_df should parse it via xmltodict into a LazyFrame
        WHEN to_df is called
        """
        mock_fs.isfile.return_value = True
        xml_content = b"<root><id>1</id><id>2</id></root>"
        mock_fs.open.return_value.__enter__.return_value = io.BytesIO(xml_content)

        df = handler.to_df("s3://test.xml")
        assert isinstance(df, pl.LazyFrame)

        # Verify data content
        result = df.collect()
        assert "root" in result.columns

    def test_from_df_raises_not_implemented(self, handler):
        """
        GIVEN a Polars DataFrame
        THEN from_df should raise NotImplementedError
        WHEN from_df is called
        """
        df = pl.DataFrame({"a": [1]})
        with pytest.raises(NotImplementedError, match="not supported"):
            handler.from_df(df, "s3://output.xml")

    def test_write_binary(self, handler, mock_fs):
        """
        GIVEN raw bytes
        THEN write should save them to the filesystem in binary mode
        WHEN write is called
        """
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__.return_value = mock_file

        data = b"<xml>data</xml>"
        handler.write(data, "s3://output.xml")

        mock_fs.open.assert_called_once_with("s3://output.xml", "wb")
        mock_file.write.assert_called_once_with(data)
