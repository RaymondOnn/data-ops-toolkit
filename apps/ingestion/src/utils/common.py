import logging
from pathlib import Path

from libs.utils.log import setup_logging

from .constants import LOG_HIGHLIGHT_KEYS


def find_path(search_dir: Path, file_pattern: str) -> Path | None:
    for path in search_dir.rglob(file_pattern):
        if path.is_dir():
            return path
    return None


def make_short_hash(length: int = 8) -> str:
    from nanoid import generate

    return generate(alphabet="0123456789abcdef", size=length)


def recursive_merge(base: dict, upd: dict) -> None:
    """Helper for nested manifest updates."""
    for k, v in upd.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            recursive_merge(base[k], v)
        else:
            base[k] = v


def setup_logger(*args, **kwargs):
    """
    Unified logging entry point.
    Configures Loguru and silences noisy third-party libraries.
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
