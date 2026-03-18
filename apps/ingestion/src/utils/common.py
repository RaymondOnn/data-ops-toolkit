
from pathlib import Path



def find_path(search_dir: Path, file_pattern: str) -> Path:
    for path in search_dir.rglob(file_pattern):
        if path.is_dir:
            return path
    raise FileNotFoundError(f"Path not found for {file_pattern}")