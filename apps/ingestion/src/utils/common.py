from functools import partial
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

setup_logger = partial(setup_logging, highlight_keys=LOG_HIGHLIGHT_KEYS)