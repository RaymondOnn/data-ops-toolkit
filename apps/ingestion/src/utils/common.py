import logging
from pathlib import Path

from libs.utils.log import setup_logging

from .constants import LOG_HIGHLIGHT_KEYS


def find_path(search_dir: Path, file_pattern: str) -> Path | None:
    """
    Recursively searches for a directory matching the given glob pattern.

    Args:
        search_dir: The root directory to start the search from.
        file_pattern: The glob pattern to match against directory names.

    Returns:
        Path | None: The first matching directory path found, or None if no
            match is found.
    """
    for path in search_dir.rglob(file_pattern):
        if path.is_dir():
            return path
    return None


def make_short_hash(length: int = 8) -> str:
    """
    Generates a short, random hexadecimal string.

    Args:
        length: The number of characters for the generated hash.

    Returns:
        str: A random string containing characters from '0-9a-f'.
    """
    from nanoid import generate

    return generate(alphabet="0123456789abcdef", size=length)


def recursive_merge(base: dict, upd: dict) -> None:
    """
    Deeply merges the 'upd' dictionary into the 'base' dictionary in-place.

    Args:
        base: The target dictionary that will be modified.
        upd: The source dictionary containing updates.
    """
    for k, v in upd.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            recursive_merge(base[k], v)
        else:
            base[k] = v


def setup_logger(*args, **kwargs):
    """
    Unified logging entry point for the application.

    Silences specific noisy third-party loggers and initializes the global
    logging system with application-specific highlight keys.

    Args:
        *args: Positional arguments passed to libs.utils.log.setup_logging.
        **kwargs: Keyword arguments passed to libs.utils.log.setup_logging.

    Returns:
        int | None: The Loguru handler ID for the console sink.
    """
    # Silence noisy third-party loggers (AWS, Ray, Scheduler, etc.)
    for logger_name in [
        "botocore",
        "boto3",
        "urllib3",
        "ray",
        "apscheduler",
        "filelock",
    ]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    return setup_logging(*args, highlight_keys=LOG_HIGHLIGHT_KEYS, **kwargs)
