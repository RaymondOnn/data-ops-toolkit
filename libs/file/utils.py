import fnmatch

from libs.file.formats.factory import FormatFactory


def filter_files(
    all_files: list[str], pattern: str | None, root_path: str
) -> list[str]:
    """
    Filters a list of files based on a glob pattern or supported extensions.

    Decision: Contract-Aware Filtering.
    If no pattern is provided, we only pick up files that our FormatFactory
    actually knows how to handle, preventing 'Unknown Format' errors.
    """
    if pattern:
        matches = fnmatch.filter(all_files, f"*{pattern}*")
        if not matches:
            raise FileNotFoundError(f"Pattern {pattern} not found in {root_path}")
        return matches

    # Filter by registered extensions (csv, parquet, etc)
    data_exts = tuple(f".{e}" for e in FormatFactory.get_supported_extensions())
    targets = [f for f in all_files if f.lower().endswith(data_exts)]

    if not targets:
        raise FileNotFoundError(f"No valid data files identified in {root_path}")

    return targets
