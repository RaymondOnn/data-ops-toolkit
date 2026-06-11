from libs.utils.dates import current_timestamp, parse_timestamp, seconds_diff


class TestDateUtils:
    """Unit tests for date and time utilities."""

    def test_current_timestamp_utc(self):
        """
        GIVEN a request for a UTC timestamp
        THEN return a datetime object with UTC offset
        WHEN current_timestamp is called with 'UTC'
        """
        ts = current_timestamp(timezone="UTC")
        assert ts.tzinfo is not None
        assert ts.utcoffset().total_seconds() == 0

    def test_current_timestamp_naive(self):
        """
        GIVEN the naive flag is enabled
        THEN return a naive datetime object
        WHEN current_timestamp is invoked
        """
        ts = current_timestamp(naive=True)
        assert ts.tzinfo is None

    def test_parse_timestamp_from_string(self):
        """
        GIVEN an ISO 8601 string
        THEN return a pendulum instance representing that time
        WHEN parse_timestamp is called
        """
        input_str = "2024-01-01T12:00:00Z"
        dt = parse_timestamp(input_str)
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 1

    def test_parse_timestamp_unix(self):
        """
        GIVEN a unix epoch float
        THEN return the correct year and month
        WHEN parse_timestamp is called
        """
        # 1704024000 is 2023-12-31 12:00:00 UTC
        dt = parse_timestamp(1704024000, naive=True)
        assert dt.year == 2023
        assert dt.month == 12
        assert dt.day == 31

    def test_seconds_diff(self):
        """
        GIVEN two timestamps separated by one hour
        THEN return exactly 3600.0 seconds
        WHEN seconds_diff is calculated
        """
        ts1 = "2024-01-01T13:00:00Z"
        ts2 = "2024-01-01T12:00:00Z"

        delta = seconds_diff(ts1, ts2)
        assert delta == 3600.0
