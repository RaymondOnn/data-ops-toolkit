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

    def __init__(self, url: str, options: dict[str, Any] | None = None):
        self.url = url.rstrip("/")
        self.options = options or {}
        self._fs = None

    @property
    @abstractmethod
    def fs(self) -> fsspec.AbstractFileSystem:
        """Get underlying fsspec filesystem."""
        raise NotImplementedError

    def open(self, path: str, mode: str = "rb") -> Any:
        """Open a file."""
        return self.fs.open(self.resolve(path), mode=mode)

    def resolve(self, path: str | Path) -> str:
        """Resolve path to absolute/fully-qualified URL."""
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
        """Check if path exists."""
        return self.fs.exists(self.resolve(str(path)))

    def ls(self, path: str = "", detail: bool = False) -> list[Any]:
        """List directory contents."""
        return self.fs.ls(self.resolve(path), detail=detail)

    def cp(self, src: str, dst: str, recursive: bool = True, **kwargs) -> None:
        """Transfer between any two paths (cross-filesystem aware)."""
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
        """Transfer remote to remote via local temporary storage."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            self.fs.get(src, tmpdir, recursive=recursive)
            self.fs.put(tmpdir, dst, recursive=recursive)

    def mv(self, src: str, dst: str, recursive: bool = True, **kwargs) -> None:
        """Move files/directories."""
        return self.fs.mv(
            self.resolve(src), self.resolve(dst), recursive=recursive, **kwargs
        )

    def rm(self, path: str, recursive: bool = False) -> None:
        """Delete files/directories."""
        return self.fs.rm(self.resolve(path), recursive=recursive)

    def walk(self, path: str, pattern: str | None = "*") -> Generator[str, None, None]:
        """Recursively walk and yield matching files."""
        pattern = pattern or "*"
        resolved = self.resolve(path)

        if not self.fs.exists(resolved):
            raise FileNotFoundError(f"Path not found: {resolved}")

        if self.fs.isfile(resolved):
            if pattern == "*" or Path(resolved).match(pattern):
                yield resolved
            return

        for p in self.fs.find(resolved):
            path_str = p[0] if isinstance(p, list) else p
            full = str(self.fs.unstrip_protocol(path_str))
            if pattern == "*" or Path(full).match(pattern):
                yield full

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
    options: dict[str, Any] | None = None,
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
    return cls(url=url, storage_options=options or {})
