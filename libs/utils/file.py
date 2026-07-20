from pathlib import PurePath


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
