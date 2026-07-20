import logging
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import suppress
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import Any

import fsspec
from upath import UPath

from libs.clients.base import BaseIOClient

LOG = logging.getLogger(__name__)


# TODO: Qurantine for file ingestion job
class FileSystemClient(BaseIOClient, ABC):
    """
    Protocol-agnostic filesystem client for local/cloud storage.

    Wraps `fsspec` to provide a consistent interface for local, S3, Azure,
    and other cloud storage providers.
    """

    def __init__(self, url: str, **options: Any):
        self.url = url.rstrip("/")
        self.options = options or {}
        self._fs = None

    @property
    @abstractmethod
    def fs(self) -> fsspec.AbstractFileSystem:
        """Get underlying fsspec filesystem.

        Returns:
            fsspec.AbstractFileSystem: The underlying fsspec filesystem.
        """
        raise NotImplementedError

    def open(self, path: str, mode: str = "rb") -> Any:
        """Open a file.

        Args:
            path: The path to open.
            mode: The mode to open the file in.

        Returns:
            Any: The file object.
        """
        return self.fs.open(self.resolve(path), mode=mode)

    def resolve(self, path: str | Path) -> str:
        """Resolve path to absolute/fully-qualified URL.

        Args:
            path: The path to resolve.

        Returns:
            str: The resolved path.
        """
        path_str = str(path)

        # Cloud protocol already present
        if "://" in path_str and not path_str.startswith("file://"):
            return str(UPath(path_str, **self.options).resolve(strict=False))

        # Local path handling
        p = Path(path_str).expanduser()
        if path_str.startswith(("./", "../", "/", "~")):
            if path_str.startswith("/") and not p.exists():
                base = UPath(self.url)
                return str((base / path_str.lstrip("/")).resolve(strict=False))
            return str(p.resolve(strict=False))

        # Relative path
        return str((UPath(self.url) / path_str).resolve(strict=False))

    def exists(self, path: str | Path) -> bool:
        """Check if path exists.

        Args:
            path: The path to check.

        Returns:
            bool: True if the path exists, False otherwise.
        """
        return self.fs.exists(self.resolve(str(path)))

    def ls(self, path: str = "", detail: bool = False) -> list[Any]:
        """List directory contents.

        Args:
            path: The path to list.
            detail: Whether to return detailed information.

        Returns:
            list[Any]: List of directory contents.
        """
        return self.fs.ls(self.resolve(path), detail=detail)

    def cp(self, src: str, dst: str, recursive: bool = True, **kwargs) -> None:
        """Transfer between any two paths (cross-filesystem aware).

        Args:
            src: The source path.
            dst: The destination path.
            recursive: Whether to transfer recursively.
        """
        src_has_protocol = "://" in src
        dst_has_protocol = "://" in dst

        if src_has_protocol and dst_has_protocol:
            # Both remote - use cp (may fail across providers)
            try:
                self.fs.cp(src, dst, recursive=recursive)
            except NotImplementedError:
                # Fallback to get/put
                self._transfer_via_local(src, dst, recursive)
        elif src_has_protocol and not dst_has_protocol:
            # Remote to local - download
            self.fs.get(src, dst, recursive=recursive)
        elif not src_has_protocol and dst_has_protocol:
            # Local to remote - upload
            self.fs.put(src, dst, recursive=recursive)
        else:
            # Both local - use shutil
            import shutil

            if recursive:
                shutil.copytree(src, dst)
            else:
                shutil.copy2(src, dst)

    def _transfer_via_local(self, src: str, dst: str, recursive: bool) -> None:
        """Transfer remote to remote via local temporary storage.

        Args:
            src: The source path.
            dst: The destination path.
            recursive: Whether to transfer recursively.
        """
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            self.fs.get(src, tmpdir, recursive=recursive)
            self.fs.put(tmpdir, dst, recursive=recursive)

    def mv(self, src: str, dst: str, recursive: bool = True, **kwargs) -> None:
        """Move files/directories.

        Args:
            src: The source path.
            dst: The destination path.
            recursive: Whether to move recursively.
        """
        return self.fs.mv(
            self.resolve(src), self.resolve(dst), recursive=recursive, **kwargs
        )

    def rm(self, path: str, recursive: bool = False) -> None:
        """Delete files/directories.

        Args:
            path: The path to delete.
            recursive: Whether to delete recursively.
        """
        return self.fs.rm(self.resolve(path), recursive=recursive)

    def glob(
        self,
        path: str | Path,
        pattern: str | None = None,
        recursive: bool = False,
        stream: bool = False,
    ) -> list[str] | Generator[str, None, None]:
        """Find paths matching a pattern, with options for recursive search and streaming.

        Args:
            path: The base directory path or a full pattern path.
            pattern: Optional glob pattern (e.g., '*.parquet') to append to the path.
            recursive: If True, enables deep nested directory matching (via /**/).
            stream: If True, returns a lazy generator instead of a list.
        """
        resolved = self.resolve(path)

        # 1. Combine path and pattern if pattern is provided
        if pattern:
            base = resolved.rstrip("/")
            leaf = pattern.lstrip("/")
            search = f"{base}/{leaf}" if base and leaf else base or leaf
        else:
            search = resolved

        # 2. Inject recursive globbing operators if requested
        if recursive and "/**/" not in search:
            if "/*" in search:
                search = search.replace("/*", "/**/", 1)
            else:
                search = search.rstrip("/") + "/**/*"

        # 3. Define the internal generator
        def _generator() -> Generator[str, None, None]:
            # If it's a direct file path without wildcards, check existence directly
            if "*" not in search:
                if self.fs.exists(search) and self.fs.isfile(search):
                    yield self.fs.unstrip_protocol(search)
                return

            # Otherwise run fsspec globbing
            for p in self.fs.glob(search, recursive=recursive):
                if self.fs.isfile(
                    p
                ):  # Standardize to only return files like walk/FormatHandler
                    yield self.fs.unstrip_protocol(p)

        if stream:
            return _generator()
        return list(_generator())

    def close(self) -> None:
        """Close the filesystem connection."""
        if self._fs is not None:
            with suppress(Exception):
                self._fs.close()

            self._fs = None
        LOG.debug(f"Closed filesystem client: {self.url}")

    def is_archive(self, path: str) -> bool:
        """Check if path points to an archive file."""
        archive_extensions = {".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz"}
        return any(path.lower().endswith(ext) for ext in archive_extensions)

    def extract_archive(
        self,
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

        resolved_path = self.resolve(archive_path)

        LOG.info(f"Extracting {archive_path} to {target_dir}")

        # Extract based on archive type
        if resolved_path.lower().endswith(".zip"):
            with zipfile.ZipFile(resolved_path, "r") as zf:
                zf.extractall(target_dir)
        elif resolved_path.lower().endswith(".tar"):
            with tarfile.open(resolved_path, "r") as tf:
                tf.extractall(target_dir)
        elif resolved_path.lower().endswith(
            ".tar.gz"
        ) or resolved_path.lower().endswith(".tgz"):
            with tarfile.open(resolved_path, "r:gz") as tf:
                tf.extractall(target_dir)
        elif resolved_path.lower().endswith(".gz"):
            # Single .gz file
            output_path = Path(target_dir) / Path(resolved_path).stem
            with gzip.open(resolved_path, "rb") as f:
                output_path.write_bytes(f.read())
        else:
            raise ValueError(f"Unsupported archive format: {archive_path}")

        return target_dir


class FileSystemSkills(Enum):
    """Mixin registry for filesystem capabilities."""

    FILE = "libs.file.mixins.data.FileMixin"
    CAS = "libs.file.mixins.cas.CASArchiveMixin"
    ARCHIVE = "libs.file.mixins.archive.StandardArchiveMixin"

    @cached_property
    def mixin_class(self):
        import importlib

        mod, cls = self.value.rsplit(".", 1)
        return getattr(importlib.import_module(mod), cls)


def create_fs_client(
    url: str,
    capabilities: set[FileSystemSkills],
    **options: Any,
) -> "FileSystemClient":
    """
    Create a filesystem client with dynamic mixin capabilities.

    Args:
        url: Root URL (e.g., 's3://bucket', '/tmp/data')
        capabilities: Set of mixins to inject
        options: Storage connection options
    """
    from .clients.azure import AzureClient
    from .clients.local import LocalClient
    from .clients.s3 import S3Client

    # Map protocol to base class
    base_map = {
        "s3://": S3Client,
        "abfs://": AzureClient,
        "az://": AzureClient,
    }

    # Find base class by protocol, default to LocalClient
    base = next(
        (client for prefix, client in base_map.items() if url.startswith(prefix)),
        LocalClient,
    )

    # Build MRO: mixins first, then base
    mixins = [cap.mixin_class for cap in capabilities]

    # Create dynamic class
    cls = type(f"Managed{base.__name__}", (base, *mixins), {})
    return cls(url=url, **options)
