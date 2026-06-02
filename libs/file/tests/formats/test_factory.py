import pytest
from libs.file.formats.csv import CSVHandler
from libs.file.formats.factory import FormatFactory


class TestFormatFactory:
    """Unit tests for the format discovery and instantiation factory."""

    def test_get_supported_extensions_returns_valid_list(self):
        """
        GIVEN the FormatFactory registry
        WHEN get_supported_extensions is called
        THEN it should return a list containing 'csv' and 'parquet'.
        """
        exts = FormatFactory.get_supported_extensions()
        assert "csv" in exts
        assert "parquet" in exts
        assert "json" in exts

    def test_get_handler_instantiates_correct_type(self):
        """
        GIVEN the 'csv' extension
        WHEN get_handler is called
        THEN it should return an instance of CSVHandler.
        """
        handler = FormatFactory.get_handler("csv")
        assert isinstance(handler, CSVHandler)

    def test_get_handler_invalid_extension_raises_error(self):
        """
        GIVEN an unsupported extension '.xlsx'
        WHEN get_handler is called
        THEN it should raise a ValueError.
        """
        with pytest.raises(ValueError, match="Unsupported file extension"):
            FormatFactory.get_handler("xlsx")
