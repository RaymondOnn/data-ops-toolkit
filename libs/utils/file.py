import os
import tempfile
from collections.abc import Generator
from contextlib import contextmanager, suppress
from pathlib import Path, PurePath
from typing import IO, Any


def is_path_like(value: str, exclude_prefixes: tuple[str, ...]) -> bool:
    """Detects if a string is semantically structured like a file or directory path.

    Excludes pure alphanumeric strings, URLs, and plain text.
    """
    if not isinstance(value, str) or not value.strip():
        return False

    # Exclude remote URLs early
    if value.lower().startswith(("http://", "https://", "s3://", "gcs://")):
        return False

    # Directly reject strings starting with blacklisted protocols/schemes
    if value.lower().startswith(exclude_prefixes):
        return False

    # Pure alphanumeric strings are likely IDs, stages, or keys, not paths
    if value.isalnum():
        return False

    path = PurePath(value)

    # Check for path characteristics:
    # 1. It is explicitly an absolute path
    # 2. It contains directory separators (more than one structural component)
    # 3. It looks like a relative file/dir reference (e.g., './config', '../data')
    return bool(
        path.is_absolute() or len(path.parts) > 1 or value.startswith((".", ".."))
    )


@contextmanager
def atomic_save(
    dest_path: str | Path,
    mode: str = "w",
    encoding: str | None = "utf-8",
    overwrite: bool = True,
    **kwargs: Any,
) -> Generator[IO[Any], None, None]:
    """
    Format-agnostic context manager for atomic file writes.

    Writes to a temporary file in the same directory, then atomically
    replaces the destination on success. On failure, the temporary file
    is cleaned up.

    Args:
        dest_path: Target file path
        mode: File open mode (supports binary with 'b')
        encoding: Text encoding (ignored for binary mode)
        overwrite: If False, raise FileExistsError when dest exists
        **kwargs: Additional arguments passed to open()

    Raises:
        FileExistsError: If overwrite=False and dest exists
        OSError: For filesystem-related errors
    """
    dest = Path(dest_path).resolve()

    # Check existence before any writes
    if not overwrite and dest.exists():
        raise FileExistsError(f"Target file already exists: '{dest}'")

    # Ensure parent directory exists
    dest.parent.mkdir(parents=True, exist_ok=True)

    # Prepare open() kwargs
    is_binary = "b" in mode
    open_kwargs: dict[str, Any] = {"mode": mode, **kwargs}
    if not is_binary and encoding is not None:
        open_kwargs["encoding"] = encoding

    # Create temporary file with proper permissions
    with tempfile.NamedTemporaryFile(
        dir=dest.parent,
        prefix=f".{dest.name}.",
        suffix=".tmp",
        delete=False,
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        with tmp_path.open(**open_kwargs) as f:
            yield f

            # Ensure all data is written to the OS buffer
            f.flush()

            # Force OS buffer to disk (if supported)
            with suppress(OSError):
                os.fsync(f.fileno())

        # Atomic replacement (rename is atomic on POSIX)
        tmp_path.replace(dest)

    except Exception:
        # Clean up temp file on any error
        with suppress(OSError):
            tmp_path.unlink(missing_ok=True)
        raise
