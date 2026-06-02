from libs.utils.dates import diff_seconds, get_current_timestamp, standardize_timestamp


class TestDateUtils:
    """Unit tests for date and time utilities."""

    def test_get_current_timestamp_utc(self):
        """
        GIVEN a request for a UTC timestamp
        THEN return a datetime object with UTC offset
        WHEN get_current_timestamp is called with 'UTC'
        """
        ts = get_current_timestamp(timezone="UTC")
        assert ts.tzinfo is not None
        assert ts.utcoffset().total_seconds() == 0

    def test_get_current_timestamp_naive(self):
        """
        GIVEN the strip_tz flag is enabled
        THEN return a naive datetime object
        WHEN get_current_timestamp is invoked
        """
        ts = get_current_timestamp(strip_tz=True)
        assert ts.tzinfo is None

    def test_standardize_timestamp_from_string(self):
        """
        GIVEN an ISO 8601 string
        THEN return a pendulum instance representing that time
        WHEN standardize_timestamp is called
        """
        input_str = "2024-01-01T12:00:00Z"
        dt = standardize_timestamp(input_str)
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 1

    def test_standardize_timestamp_unix(self):
        """
        GIVEN a unix epoch float
        THEN return the correct year and month
        WHEN standardize_timestamp is called
        """
        # 1704024000 is 2023-12-31 12:00:00 UTC
        dt = standardize_timestamp(1704024000, force_naive=True)
        assert dt.year == 2023
        assert dt.month == 12
        assert dt.day == 31

    def test_diff_seconds(self):
        """
        GIVEN two timestamps separated by one hour
        THEN return exactly 3600.0 seconds
        WHEN diff_seconds is calculated
        """
        ts1 = "2024-01-01T13:00:00Z"
        ts2 = "2024-01-01T12:00:00Z"

        delta = diff_seconds(ts1, ts2)
        assert delta == 3600.0
