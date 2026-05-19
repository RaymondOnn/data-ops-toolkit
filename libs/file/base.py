import logging
from abc import ABC, abstractmethod
from collections.abc import Generator
from enum import Enum
from pathlib import Path
from typing import Any, cast

import fsspec
from upath import UPath

from libs.clients.base import BaseIOClient

LOG = logging.getLogger(__name__)


# TODO: Qurantine for file ingestion job
class FileSystemClient(BaseIOClient, ABC):
    def __init__(self, url: str, storage_options: dict[str, Any] | None = None):
        self.url = url.rstrip("/")
        self.opts = storage_options or {}
        self._fs = None

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

    def resolve_path(self, path: str | Path) -> str:
        """
        CLI-style path resolution using UPath to preserve cloud protocols.
        """
        path_str = str(path)

        # 1. Cloud Protocol Bypass
        # If it's s3://, abfs://, etc., return as-is
        if "://" in path_str and not path_str.startswith("file://"):
            return str(UPath(path_str, **self.opts).resolve(strict=False))

        # 2. Local Path Handling
        # Expand user (~) and resolve absolute path
        # .resolve(strict=False) allows us to resolve paths that don't exist yet
        p = Path(path_str).expanduser()

        # Handle Explicit Relative (./ or ../) or Absolute (/)
        if path_str.startswith(("./", "../", "/", "~")):
            # If it's an absolute path that doesn't exist (monorepo virtual path)
            if path_str.startswith("/") and not p.exists():
                # Treat as project-root relative
                # Pass self.opts to ensure protocol-specific settings (like LocalStack endpoints) are respected
                base = UPath(self.url)
                return str((base / path_str.lstrip("/")).resolve(strict=False))

            return str(p.resolve(strict=False))

        # 3. Naked Relative Paths (e.g., "samples/data.csv")
        # Join to the client's base URL (self.url)
        base = UPath(self.url)
        return str((base / path_str).resolve(strict=False))

    def walk_paths(self, path: str, pattern: str = "*") -> Generator[str, None, None]:
        """
        Discovery utility:
        - If path is a file, yields it.
        - If path is a folder, yields all files matching the pattern.
        """
        resolved = self.resolve_path(path)

        if not self.fs.exists(resolved):
            raise FileNotFoundError(f"Path not found: {resolved}")

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

    def ls(self, path: str = "", detail: bool = False) -> list[Any]:
        return self.fs.ls(self.resolve_path(path), detail=detail)

    def cp(
        self, source: str, destination: str, recursive: bool = True, **kwargs
    ) -> None:
        src = self.resolve_path(source)
        dst = self.resolve_path(destination)
        # s3fs and local fsspec both support recursive cp
        return self.fs.cp(src, dst, recursive=recursive, **kwargs)

    def mv(
        self, source: str, destination: str, recursive: bool = True, **kwargs
    ) -> None:
        src = self.resolve_path(source)
        dst = self.resolve_path(destination)
        return self.fs.mv(src, dst, recursive=recursive, **kwargs)

    def rm(self, path: str, recursive: bool = False) -> None:
        return self.fs.rm(self.resolve_path(path), recursive=recursive)


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
