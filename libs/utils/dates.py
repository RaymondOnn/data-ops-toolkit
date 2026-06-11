"""Advanced datetime utilities with pendulum."""

from datetime import datetime
from typing import Any

import pendulum


def current_timestamp(timezone: str | None = None, naive: bool = False) -> datetime:
    """Get current timestamp, optionally naive."""
    now = pendulum.now(timezone)
    return now.naive() if naive else now


def parse_timestamp(
    ts: Any, timezone: str | None = None, naive: bool = True
) -> pendulum.DateTime:
    """Parse various timestamp formats into pendulum.DateTime."""
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
    """Calculate (ts1 - ts2) in seconds."""
    dt1 = parse_timestamp(ts1, timezone=timezone)
    dt2 = parse_timestamp(ts2, timezone=timezone)
    return (dt1 - dt2).total_seconds()
