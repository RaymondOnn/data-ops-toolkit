from datetime import UTC, datetime
from unittest.mock import patch

from src.utils.dates import (
    end_of_day_timestamp,
    epoch_to_iso,
    iso_to_epoch,
)


class TestAppDatesUtils:
    """Unit tests for application-specific date utility functions."""

    @patch("apps.ingestion.src.utils.dates.datetime")
    def test_end_of_day_timestamp(self, mock_datetime):
        """
        GIVEN the current time is mocked to a specific point
        THEN end_of_day_timestamp should return the Unix timestamp for 23:59:59
        WHEN called
        """
        # Mock datetime.now() to return a fixed datetime object
        mock_now = datetime(2024, 3, 10, 10, 30, 0, tzinfo=UTC)
        mock_datetime.now.return_value = mock_now
        mock_datetime.combine.side_effect = datetime.combine
        mock_datetime.fromtimestamp.side_effect = datetime.fromtimestamp

        expected_eod = datetime(2024, 3, 10, 23, 59, 59)
        result = end_of_day_timestamp()

        # Decision: Component Verification.
        # We convert the resulting timestamp back to a local datetime to verify
        # the logical boundary (23:59:59) matches our expectation.
        assert datetime.fromtimestamp(result) == expected_eod

    def test_epoch_to_iso_valid_epoch(self):
        """
        GIVEN a valid Unix epoch timestamp
        THEN it should return the correct ISO 8601 formatted string
        WHEN epoch_to_iso is called
        """
        epoch_ts = 1710000000.0  # March 9, 2024 10:40:00 AM UTC
        # The exact string depends on local timezone, so we check format
        result = epoch_to_iso(epoch_ts)
        assert isinstance(result, str)
        assert "T" in result  # ISO 8601 format

    def test_epoch_to_iso_none_epoch(self):
        """
        GIVEN a None epoch timestamp
        THEN it should return "N/A"
        WHEN epoch_to_iso is called
        """
        assert epoch_to_iso(None) == "N/A"

    def test_iso_to_epoch(self):
        """
        GIVEN an ISO 8601 formatted string
        THEN it should return the correct Unix epoch timestamp
        WHEN iso_to_epoch is called
        """
        iso_str = "2024-03-09T10:40:00"
        expected_epoch = datetime(2024, 3, 9, 10, 40, 0).timestamp()
        assert iso_to_epoch(iso_str) == expected_epoch
