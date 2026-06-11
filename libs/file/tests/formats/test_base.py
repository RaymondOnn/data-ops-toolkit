import io
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from libs.file.formats.base import FormatHandler


# Concrete implementation for testing the abstract base class
class MockFormatHandler(FormatHandler):
    """A mock implementation of FormatHandler for testing purposes."""

    def discover(self, input_path: Path | str, pattern: str | None = None) -> set[str]:
        return {"mock_path/file.txt"}

    def to_df(self, input_path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        return pl.LazyFrame({"col": [1, 2]})

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_path: Path | str) -> None:
        pass

    def read(self, input_path: Path | str, **kwargs: Any) -> io.BytesIO:
        return io.BytesIO(b"mock data")

    def write(self, data: bytes, output_path: Path | str) -> None:
        pass


class TestFormatHandler:
    """Unit tests for the abstract FormatHandler base class."""

    @pytest.fixture
    def mock_fs(self):
        """Returns a MagicMock for fsspec.AbstractFileSystem."""
        return MagicMock(spec=AbstractFileSystem)

    def test_init_with_defaults(self):
        """
        GIVEN no filesystem or storage options are provided
        THEN it should initialize with a default local filesystem and empty options
        WHEN FormatHandler is instantiated
        """
        with patch("fsspec.filesystem") as mock_fsspec:
            mock_fsspec.return_value = MagicMock(spec=AbstractFileSystem)
            handler = MockFormatHandler()
            mock_fsspec.assert_called_once_with("file")
            assert isinstance(handler.fs, MagicMock)
            assert handler.options == {}

    def test_init_with_custom_fs_and_options(self, mock_fs):
        """
        GIVEN a custom filesystem and storage options
        THEN it should initialize with the provided values
        WHEN FormatHandler is instantiated
        """
        custom_options = {"key": "value"}
        handler = MockFormatHandler(fs=mock_fs, storage_options=custom_options)
        assert handler.fs == mock_fs
        assert handler.options == custom_options

    def test_is_splittable_default(self):
        """
        GIVEN a base FormatHandler
        THEN is_splittable should return False by default
        WHEN the property is accessed
        """
        handler = MockFormatHandler()
        assert handler.is_splittable is False

    def test_context_manager_exit_closes_fs(self, mock_fs):
        """
        GIVEN a FormatHandler with a mock filesystem that has a close method
        THEN the fs.close() method should be called
        WHEN the handler exits its context
        """
        handler = MockFormatHandler(fs=mock_fs)
        with handler:
            pass
        mock_fs.close.assert_called_once()

    def test_context_manager_exit_no_close_method(self):
        """
        GIVEN a FormatHandler with a mock filesystem without a close method
        THEN no error should be raised
        WHEN the handler exits its context
        """
        mock_fs_no_close = MagicMock(spec=AbstractFileSystem, close=None)
        handler = MockFormatHandler(fs=mock_fs_no_close)
        with handler:
            pass
        # No exception should be raised, and close should not be called
        assert not hasattr(mock_fs_no_close, "close") or mock_fs_no_close.close is None
