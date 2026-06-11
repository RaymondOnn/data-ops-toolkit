from datetime import datetime
from datetime import time as dt_time


def end_of_day_timestamp() -> float:
    """Returns the Unix timestamp for 23:59:59 of the current day.

    This is typically used to set 'expires_at' values for snapshot jobs,
    ensuring they expire at the end of the day they were created.

    Returns:
        float: The Unix timestamp (seconds since epoch) for the end of the
            current day in the local timezone.
    """
    now = datetime.now().astimezone()
    # Combine today's date with the last possible second of the day
    eod = datetime.combine(now.date(), dt_time(23, 59, 59))
    return eod.timestamp()


def epoch_to_iso(epoch: float | None) -> str:
    """Converts a Unix epoch (float) to a human-readable ISO 8601 string.

    Example: 1709731199.0 -> "2026-03-06T23:59:59" (local timezone)

    Args:
        epoch: The Unix timestamp (seconds since epoch). Can be None.

    Returns:
        str: The ISO 8601 formatted string, or "N/A" if epoch is None.
    """
    if epoch is None:
        return "N/A"
    # Using local time for logging/UI clarity
    return datetime.fromtimestamp(epoch).isoformat()


def iso_to_epoch(iso_str: str) -> float:
    """Converts an ISO 8601 formatted string back into a Unix epoch.

    This is useful for parsing timestamps received from external APIs or
    manual configurations back into a numerical format.

    Args:
        iso_str: The ISO 8601 formatted string (e.g., "2026-03-06T23:59:59").

    Returns:
        float: The Unix timestamp (seconds since epoch).
    """
    return datetime.fromisoformat(iso_str).timestamp()
