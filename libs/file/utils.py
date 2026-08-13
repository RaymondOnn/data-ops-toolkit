import fnmatch
import logging
from pathlib import Path
from typing import IO

from upath import UPath

from libs.file.formats.factory import FormatFactory

LOG = logging.getLogger(__name__)


def filter_files(
    all_files: list[str], pattern: str | None, root_path: str
) -> list[str]:
    """
    Filters a list of files based on a glob pattern or supported extensions.

    Notes:
    - If no pattern is provided, we only pick up files that our FormatFactory
      actually knows how to handle, preventing 'Unknown Format' errors.
    """
    if pattern:
        matches = fnmatch.filter(all_files, f"*{pattern}*")
        if not matches:
            raise FileNotFoundError(f"Pattern {pattern} not found in {root_path}")
        return matches

    # Filter by registered extensions (csv, parquet, etc)
    data_exts = tuple(f".{e}" for e in FormatFactory.supported_extensions())
    targets = [f for f in all_files if f.lower().endswith(data_exts)]

    if not targets:
        raise FileNotFoundError(f"No valid data files identified in {root_path}")

    return targets


def get_file_ext(file_path: str) -> str:
    """Extracts clean lower-case extension from standard, cloud, or virtual archive paths."""
    clean_path = file_path.replace("zip://", "").replace("archive://", "")
    if "!!" in clean_path:
        clean_path = clean_path.split("!!")[-1]

    clean_path = clean_path.rstrip("*")

    try:
        path = UPath(clean_path)

        # 1. Check if the path targets a Delta Lake directory structure
        if path.is_dir() and (path / "_delta_log").exists():
            return "delta"

        # 2. Check if the path targets an Iceberg metadata file or metadata folder
        if "metadata.json" in path.name or (
            path.is_dir() and (path / "metadata").exists()
        ):
            return "iceberg"

        return path.suffix.lstrip(".").lower()
    except Exception:
        return Path(clean_path).suffix.lstrip(".").lower()


def detect_encoding(path: str) -> tuple[IO[bytes], str]:
    """Detect file encoding using charset-normalizer.

    Args:
        path: The path to the file.

    Returns:
        tuple[io.IOBase, str]: The file object and the encoding.
    """
    from charset_normalizer import from_bytes

    with UPath(path).open("rb") as stream:
        sample = stream.read(4096)
        result = from_bytes(sample).best()
        encoding = result.encoding if result else "utf-8"
        return stream, encoding


def extract_archive(
    archive_path: str,
    target_dir: str | None = None,
) -> str:
    """
    Extract archive to a directory.

    Args:
        archive_path: Path to the archive file
        target_dir: Optional target directory (creates temp if not provided)

    Returns:
        Path to the extraction directory
    """
    import gzip
    import tarfile
    import tempfile
    import zipfile

    target_dir = tempfile.mkdtemp(dir=target_dir, prefix="archive_extract_")

    # resolved_path = self.resolve(archive_path)
    resolved_path = archive_path.casefold()

    LOG.info(f"Extracting {archive_path} to {target_dir}")

    # Extract based on archive type
    match resolved_path:
        case _ if resolved_path.endswith(".zip"):
            with zipfile.ZipFile(resolved_path, "r") as zf:
                zf.extractall(target_dir)

        case _ if resolved_path.endswith(".tar"):
            with tarfile.open(resolved_path, "r") as tf:
                tf.extractall(target_dir)

        case _ if resolved_path.endswith((".tar.gz", ".tgz")):
            with tarfile.open(resolved_path, "r:gz") as tf:
                tf.extractall(target_dir)

        case _ if resolved_path.endswith(".gz"):
            # Single .gz file
            output_path = Path(target_dir) / Path(resolved_path).stem
            with gzip.open(resolved_path, "rb") as f:
                output_path.write_bytes(f.read())

        case _:
            raise ValueError(f"Unsupported archive format: {archive_path}")

    return target_dir
