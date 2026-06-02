from datetime import datetime
from typing import TYPE_CHECKING, Any

import pendulum

if TYPE_CHECKING:
    from pendulum.interval import Interval


def get_current_timestamp(
    timezone: str | None = None,
    strip_tz: bool = False,
) -> datetime:
    """
    Generates a current timestamp with robust support for pipeline logic.

    Args:
        timezone: The timezone name (e.g., 'UTC', 'Asia/Singapore').
        strip_tz: If True, returns a naive datetime object.

    Returns:
        datetime: The current timestamp.
    """
    # Pendulum handles None by using the system local timezone automatically
    now = pendulum.now(timezone)

    # Optionally strip the timezone info (Naive for ClickHouse)
    if strip_tz:
        return now.naive()

    return now


def standardize_timestamp(
    ts: Any, timezone: str | None = None, force_naive: bool = True
) -> pendulum.DateTime:
    """
    Standardizes various datetime inputs into a pendulum instance.

    Args:
        ts: The timestamp input (string, int, float, or datetime).
        timezone: Optional target timezone for aware datetimes.
        force_naive: If True, strips timezone info before returning.

    Returns:
        pendulum.DateTime: A standardized pendulum datetime object.

    Raises:
        ValueError: If the input cannot be parsed as a valid datetime.
    """
    if isinstance(ts, str):
        dt = pendulum.parse(ts)
    elif isinstance(ts, int | float):
        dt = pendulum.from_timestamp(ts)
    else:
        dt = pendulum.instance(ts)

    # If it's not a full DateTime, we can't safely proceed
    if not isinstance(dt, pendulum.DateTime):
        raise ValueError(f"Input '{ts}' is a {type(dt).__name__}, not a full DateTime.")

    if timezone:
        return dt.in_tz(timezone)

    return dt.naive() if force_naive else dt


def diff_seconds(ts1: Any, ts2: Any, timezone: str | None = None) -> float:
    """
    Safely calculates (ts1 - ts2) in seconds, handling naive/aware mismatches
    by standardizing both to the same reference.

    Args:
        ts1: The first timestamp (minuend).
        ts2: The second timestamp (subtrahend).
        timezone: Contextual timezone for standardization.

    Returns:
        float: The difference in seconds.
    """
    dt1 = standardize_timestamp(ts1, timezone=timezone)
    dt2 = standardize_timestamp(ts2, timezone=timezone)

    diff: Interval[datetime] = dt1 - dt2
    return diff.total_seconds()
