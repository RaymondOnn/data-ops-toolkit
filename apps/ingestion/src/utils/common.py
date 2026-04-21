from pathlib import Path

from nanoid import generate


def find_path(search_dir: Path, file_pattern: str) -> Path:
    for path in search_dir.rglob(file_pattern):
        if path.is_dir():
            return path
    raise FileNotFoundError(f"Path not found for {file_pattern}")


def make_short_hash(length: int = 8) -> str:
    return generate(alphabet="0123456789abcdef", size=length)
