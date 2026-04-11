import logging
from abc import ABC, abstractmethod
from collections.abc import Generator
from enum import Enum
from pathlib import Path
from typing import Any, cast

import fsspec

from libs.clients.base import BaseIOClient

LOG = logging.getLogger(__name__)


# TODO: Qurantine for file ingestion job
class FileSystemClient(BaseIOClient, ABC):
    def __init__(self, url: str, storage_options: dict[str, Any] | None = None):
        self.url = url.rstrip("/")
        self.opts = storage_options or {}
        self._fs: fsspec.AbstractFileSystem | None = None

    @property
    @abstractmethod
    def fs(self) -> fsspec.AbstractFileSystem:
        raise NotImplementedError("Subclasses must implement connect() method.")

    def open(self, path: str, mode: str = "rb") -> Any:
        """Standard file open delegating to the fsspec filesystem."""
        return self.fs.open(self.resolve_path(path), mode=mode)

    def close(self) -> None:
        """FileSystem clients are generally stateless wrappers around fsspec."""
        pass

    # ?: Perhaps quarantine file as a separate method
    # def validate_integrity(self, path: str) -> bool:
    #     """Self-healing: Checks for 0-byte files and moves them to quarantine."""
    #     ful l_path = self._get_full_path(path)
    #     if not self.fs.exists(full_path):
    #         return False

    #     if self.fs.size(full_path) == 0:
    #         LOG.error("Zero-byte file detected", extra={"path": full_path, "event": "quarantine"})
    #         quarantine_path = f"{self.url}/quarantine/{path.split('/')[-1]}"
    #         self.fs.makedirs(self.fs._parent(quarantine_path), exist_ok=True)
    #         self.fs.move(full_path, quarantine_path)
    #         return False
    #     return True

    def resolve_path(self, path: str) -> str:
        """The fsspec equivalent of Path.resolve()."""
        if (
            "://" in path or self.url.startswith(("s3://", "abfs://", "az://"))
        ) and self.fs:
            stripped = self.fs._strip_protocol(path)
            path_str = stripped[0] if isinstance(stripped, list) else stripped

            # Use fsspec's internal path cleaning to handle dots and double slashes
            clean_path = self.fs._parent(path_str + "/a")
            return str(self.fs.unstrip_protocol(clean_path))
        return str(Path(path).resolve())

    def walk_paths(self, path: str, pattern: str = "*") -> Generator[str, None, None]:
        """
        Discovery utility:
        - If path is a file, yields it.
        - If path is a folder, yields all files matching the pattern.
        """
        resolved = self.resolve_path(path)

        if not self.fs.exists(resolved):
            LOG.warning(f"Path does not exist: {resolved}")
            return

        if self.fs.isfile(resolved):
            if pattern == "*" or Path(resolved).match(pattern):
                yield resolved
        else:
            # Use fs.find() for optimized recursive discovery.
            # fs.find() returns a dict of path: info or a list of paths.
            # It is generally much more performant than glob for cloud providers.
            for p in self.fs.find(resolved):
                path_str = p[0] if isinstance(p, list) else p
                full_path = str(self.fs.unstrip_protocol(path_str))
                if pattern == "*" or Path(full_path).match(pattern):
                    yield full_path

    def smart_transfer(self, local_source: str, remote_dest: str) -> None:
        """Handles local-to-cloud or cloud-to-cloud transfers safely."""
        if "://" in local_source and "://" in remote_dest:
            # Remote to Remote (e.g., S3 to S3)
            self.fs.cp(local_source, remote_dest)
        else:
            # Local to Remote (Upload)
            self.fs.put(local_source, remote_dest)

    def exists(self, path: str | Path) -> bool:
        return self.fs.exists(self.resolve_path(str(path)))

    def info(self, path: str | Path) -> dict[str, Any]:
        """Returns detailed metadata (size, mtime, type) using fsspec."""
        return self.fs.info(self.resolve_path(str(path)))

    def delete_dir(self, path: str | Path) -> None:
        full_path = self.resolve_path(str(path))
        if self.fs.exists(full_path):
            self.fs.rm(full_path, recursive=True)

    def move_dir(self, source_path: str | Path, target_path: str | Path) -> None:
        self.fs.mv(
            self.resolve_path(str(source_path)),
            self.resolve_path(str(target_path)),
            recursive=True,
        )

    def copy_dir(self, source_path: str | Path, target_path: str | Path) -> None:
        self.fs.cp(
            self.resolve_path(str(source_path)),
            self.resolve_path(str(target_path)),
            recursive=True,
        )


class FileSystemSkills(Enum):
    FILE = ("file", "libs.file.mixins.data.FlatFileMixin")
    CAS = ("cas", "libs.file.mixins.cas.CASArchiveMixin")
    ARCHIVE = ("archive", "libs.file.mixins.archive.StandardArchiveMixin")

    def __init__(self, key: str, class_path: str):
        self.key = key
        self.class_path = class_path

    @property
    def mixin_class(self):
        """Dynamic import to keep the 2GB RAM footprint small."""
        import importlib

        module_path, class_name = self.class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)


def create_fs_client(
    url: str,
    capabilities: set[FileSystemSkills],
    storage_options: dict[str, Any] | None = None,
) -> FileSystemClient:
    """
    Assembles a Managed Client with dynamic capabilities (Ingestion, Archive, etc.).
    """
    from .clients.azure import AzureClient
    from .clients.local import LocalClient
    from .clients.s3 import S3Client

    # 1. Map Protocol to Base Class
    protocol_map = {
        "s3://": S3Client,
        # "gs://": GCSClient,
        # "gcs://": GCSClient,
        "abfs://": AzureClient,
        "az://": AzureClient,
    }

    # Default to LocalClient if no cloud protocol is detected
    base_class = cast("type[FileSystemClient]", LocalClient)
    for prefix, cls in protocol_map.items():
        if url.startswith(prefix):
            base_class = cast("type[FileSystemClient]", cls)
            break

    # 2. Collect Mixins from Enum
    bases = [base_class]
    for cap in capabilities:
        bases.append(cap.mixin_class)

    # 3. Create Dynamic Managed Type
    class_name = f"Managed{base_class.__name__}"
    ManagedClientClass = type(class_name, tuple(bases), {})

    # 4. Instantiate (triggers super().__init__ and fsspec setup)
    return cast(
        "FileSystemClient", ManagedClientClass(url=url, storage_options=storage_options)
    )
