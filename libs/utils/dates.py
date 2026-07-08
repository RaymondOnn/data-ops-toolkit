"""Advanced datetime utilities with pendulum."""

from datetime import datetime, timedelta
from typing import Any

import pendulum


def current_timestamp(timezone: str | None = None, naive: bool = False) -> datetime:
    """Get current timestamp, optionally naive.

    Args:
        timezone: The timezone to use for parsing timestamps.
        naive: Whether to return a naive datetime.

    Returns:
        datetime: The current timestamp.
    """
    now = pendulum.now(timezone)
    return now.naive() if naive else now


def parse_timestamp(
    ts: Any, timezone: str | None = None, naive: bool = True
) -> pendulum.DateTime:
    """Parse various timestamp formats into pendulum.DateTime.

    Args:
        ts: The timestamp to parse.
        timezone: The timezone to use for parsing timestamps.
        naive: Whether to return a naive datetime.

    Returns:
        pendulum.DateTime: The parsed timestamp.
    """
    if isinstance(ts, str):
        dt = pendulum.parse(ts)
    elif isinstance(ts, int | float):
        dt = pendulum.from_timestamp(ts)
    else:
        dt = pendulum.instance(ts)

    if not isinstance(dt, pendulum.DateTime):
        raise ValueError(f"Cannot parse {ts} as DateTime")

    dt = dt.in_tz(timezone) if timezone else dt
    return dt.naive() if naive else dt


def seconds_diff(ts1: Any, ts2: Any, timezone: str | None = None) -> float:
    """Calculate (ts1 - ts2) in seconds.

    Args:
        ts1: The first timestamp.
        ts2: The second timestamp.
        timezone: The timezone to use for parsing timestamps.

    Returns:
        float: The difference in seconds between ts1 and ts2.
    """
    dt1 = parse_timestamp(ts1, timezone=timezone)
    dt2 = parse_timestamp(ts2, timezone=timezone)
    return (dt1 - dt2).total_seconds()


def parse_duration(value: str) -> timedelta:
    """Parse duration strings like '5m', '1h', '30s'.

    Args:
        value: The duration string to parse.

    Returns:
        timedelta: The parsed duration.
    """
    import re

    match = re.match(r"^(\d+)([smhd])$", value)
    if not match:
        raise ValueError(f"Invalid duration format: {value}")

    num = int(match.group(1))
    unit = match.group(2)

    multipliers = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
    }
    return timedelta(seconds=num * multipliers[unit])
