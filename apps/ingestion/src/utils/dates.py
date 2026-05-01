from datetime import datetime
from datetime import time as dt_time
from zoneinfo import ZoneInfo


def get_end_of_day_ts() -> float:
    """
    Returns the Unix timestamp for 23:59:59 of the current day.
    Used to set the 'expires_at' value for snapshot jobs.
    """
    now = datetime.now().astimezone()
    # Combine today's date with the last possible second of the day
    eod = datetime.combine(now.date(), dt_time(23, 59, 59))
    return eod.timestamp()



def epoch_to_iso(epoch: float | None) -> str:
    """
    Converts a Unix epoch (float) to a human-readable ISO 8601 string.
    Example: 1709731199.0 -> "2026-03-06T23:59:59"
    """
    if epoch is None:
        return "N/A"
    # Using local time for logging/UI clarity
    return datetime.fromtimestamp(epoch).isoformat()


def iso_to_epoch(iso_str: str) -> float:
    """
    Converts an ISO format string back into a Unix epoch.
    Useful if reading timestamps from an external API or manual config.
    """
    dt = datetime.fromisoformat(iso_str)
    return dt.timestamp()




