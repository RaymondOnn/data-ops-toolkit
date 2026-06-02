import logging
from abc import ABC, abstractmethod
from collections.abc import Generator
from enum import Enum
from pathlib import Path
from typing import Any, cast

import fsspec
from upath import UPath

from libs.clients.base import BaseIOClient
from libs.file.utils import filter_files

LOG = logging.getLogger(__name__)


# TODO: Qurantine for file ingestion job
class FileSystemClient(BaseIOClient, ABC):
    """
    Abstract base class for protocol-agnostic filesystem interactions.

    Wraps `fsspec` to provide a consistent interface for local, S3, Azure,
    and other cloud storage providers.
    """

    def __init__(self, url: str, storage_options: dict[str, Any] | None = None):
        """
        Initializes the FileSystemClient.

        Args:
            url: The base URL or root directory for the client.
            storage_options: Driver-specific configuration (e.g., credentials).
        """
        self.url = url.rstrip("/")
        self.opts = storage_options or {}
        self._fs = None

    @property
    @abstractmethod
    def fs(self) -> fsspec.AbstractFileSystem:
        """
        Returns the underlying fsspec filesystem instance.

        Returns:
            fsspec.AbstractFileSystem: An initialized filesystem driver.
        """
        raise NotImplementedError("Subclasses must implement connect() method.")

    def open(self, path: str, mode: str = "rb") -> Any:
        """
        Opens a file using the underlying filesystem.

        Args:
            path: Relative or absolute path to the file.
            mode: Standard Python file mode (e.g., 'rb', 'wb').

        Returns:
            Any: A file-like object compatible with Python's IO interface.
        """
        return self.fs.open(self.resolve_path(path), mode=mode)

    def close(self) -> None:
        """
        Cleans up resources. FileSystem clients are generally stateless.
        """
        pass

    def resolve_path(self, path: str | Path) -> str:
        """
        Resolves a path string into a fully qualified URL or absolute path.

        Handles cloud protocols (s3://), local absolute paths, and relative
        paths joined against the client's base URL.

        Args:
            path: The input path string or Path object to resolve.

        Returns:
            str: A resolved, protocol-aware path string.
        """
        path_str = str(path)

        # 1. Cloud Protocol Bypass
        if "://" in path_str and not path_str.startswith("file://"):
            return str(UPath(path_str, **self.opts).resolve(strict=False))

        # 2. Local Path Handling
        p = Path(path_str).expanduser()

        # Handle Explicit Relative (./ or ../) or Absolute (/)
        if path_str.startswith(("./", "../", "/", "~")):
            # If it's an absolute path that doesn't exist (monorepo virtual path)
            if path_str.startswith("/") and not p.exists():
                base = UPath(self.url)
                return str((base / path_str.lstrip("/")).resolve(strict=False))

            return str(p.resolve(strict=False))

        # 3. Naked Relative Paths (e.g., "samples/data.csv")
        base = UPath(self.url)
        return str((base / path_str).resolve(strict=False))

    def walk_paths(self, path: str, pattern: str = "*") -> Generator[str, None, None]:
        """
        Recursively discovers files under a specific path.

        If the path is a file, it yields the file. If it is a directory, it
        yields all children matching the provided glob pattern.

        Args:
            path: The root path to search.
            pattern: Glob pattern to filter files (default '*').

        Yields:
            str: Fully qualified path strings for discovered files.

        Raises:
            FileNotFoundError: If the input path does not exist.
        """
        resolved = self.resolve_path(path)

        if not self.fs.exists(resolved):
            raise FileNotFoundError(f"Path not found: {resolved}")

        if self.fs.isfile(resolved):
            if pattern == "*" or Path(resolved).match(pattern):
                yield resolved
        else:
            # Use fs.find() for optimized recursive discovery.
            for p in self.fs.find(resolved):
                path_str = p[0] if isinstance(p, list) else p
                full_path = str(self.fs.unstrip_protocol(path_str))
                if pattern == "*" or Path(full_path).match(pattern):
                    yield full_path

    def smart_transfer(self, local_source: str, remote_dest: str) -> None:
        """
        Optimized transfer between local and remote filesystems.

        Args:
            local_source: Path to the source file (local or remote URL).
            remote_dest: Path to the destination (usually a remote URL).
        """
        if "://" in local_source and "://" in remote_dest:
            # Remote to Remote (e.g., S3 to S3)
            self.fs.cp(local_source, remote_dest)
        else:
            # Local to Remote (Upload)
            self.fs.put(local_source, remote_dest)

    def exists(self, path: str | Path) -> bool:
        """
        Checks if a path exists on the filesystem.

        Args:
            path: Path or URL to check.

        Returns:
            bool: True if the path exists, False otherwise.
        """
        return self.fs.exists(self.resolve_path(str(path)))

    def info(self, path: str | Path) -> dict[str, Any]:
        """
        Retrieves detailed metadata for a file or directory.

        Args:
            path: Path or URL to inspect.

        Returns:
            dict[str, Any]: Metadata including size, mtime, and type.
        """
        return self.fs.info(self.resolve_path(str(path)))

    def ls(self, path: str = "", detail: bool = False) -> list[Any]:
        """
        Lists the contents of a directory.

        Args:
            path: Path or URL to list.
            detail: If True, returns metadata dictionaries for each item.

        Returns:
            list[Any]: A list of paths or metadata dictionaries.
        """
        return self.fs.ls(self.resolve_path(path), detail=detail)

    def cp(
        self, source: str, destination: str, recursive: bool = True, **kwargs
    ) -> None:
        """
        Copies files or directories.

        Args:
            source: Source path or URL.
            destination: Destination path or URL.
            recursive: If True, performs a recursive copy.
            **kwargs: Additional driver-specific options.
        """
        src = self.resolve_path(source)
        dst = self.resolve_path(destination)
        # s3fs and local fsspec both support recursive cp
        return self.fs.cp(src, dst, recursive=recursive, **kwargs)

    def mv(
        self, source: str, destination: str, recursive: bool = True, **kwargs
    ) -> None:
        """
        Moves or renames files or directories.

        Args:
            source: Source path or URL.
            destination: Destination path or URL.
            recursive: If True, performs a recursive move.
            **kwargs: Additional driver-specific options.
        """
        src = self.resolve_path(source)
        dst = self.resolve_path(destination)
        return self.fs.mv(src, dst, recursive=recursive, **kwargs)

    def rm(self, path: str, recursive: bool = False) -> None:
        """
        Deletes a file or directory.

        Args:
            path: Path or URL to remove.
            recursive: If True, deletes directories and their contents.
        """
        return self.fs.rm(self.resolve_path(path), recursive=recursive)

    def _mount_archive_fs(self, path: str) -> fsspec.AbstractFileSystem:
        """Resolves and mounts the appropriate filesystem layer for a path.

        If the path refers to a supported archive (zip, tar, etc.), a virtual
        filesystem is returned. Otherwise, it defaults to the base filesystem.

        Args:
            path: The physical or logical path to mount.

        Returns:
            fsspec.AbstractFileSystem: An initialized filesystem instance.

        Decision: Decoupled Storage Mounting.
        Separating mounting from discovery allows the system to verify
        connectivity without the overhead of recursive listing.
        """
        root_to_mount = self.resolve_path(path)

        archive_map = {
            ".zip": "zip",
            ".tar": "tar",
            ".tar.gz": "tar",
            ".tgz": "tar",
            ".gz": "gzip",
        }

        # Detect Archive Type based on the mounted path
        ext = next((e for e in archive_map if root_to_mount.lower().endswith(e)), None)

        if ext:
            LOG.debug(f"Mounting virtual filesystem for archive: {root_to_mount}")
            return fsspec.filesystem(
                archive_map[ext], fo=root_to_mount, remote_options=self.opts
            )

        return self.fs

    def get_archive_contents(
        self, archive_path: str, pattern: str | None = None
    ) -> list[str]:
        """Discovers and filters files within an archive container.

        Args:
            archive_path: Path to the physical archive.
            pattern: Optional glob pattern for filtering.

        Returns:
            list[str]: A list of filtered file paths within the archive.
        """
        fs = self._mount_archive_fs(archive_path)
        # Find all files in the archive (virtual FS root is the archive root)
        all_files = fs.find("")
        return filter_files(all_files, pattern, archive_path)

    # --- SKILLS INTERFACE (Implemented by Mixins) ---
    # Decision: Centralized Contract.
    # We define stubs here so the Orchestrator can call these methods on any
    # FileSystemClient instance. Concrete implementations are provided by
    # Mixins. If a skill is missing, we raise a descriptive error.

    def resolve_format_handler(
        self, target_path: str, archive_path: str | None = None
    ) -> Any:
        """Stub for format discovery. Requires FILE skill."""
        raise NotImplementedError(
            f"Client {self.url} does not support format resolution (FILE skill missing)."
        )

    def verify_file_viability(self, fs: fsspec.AbstractFileSystem, target: str) -> bool:
        """Stub for sanity checks. Requires FILE skill."""
        raise NotImplementedError("verify_file_viability requires the FILE skill.")

    def ingest_to_lazyframe(
        self,
        path_or_list: str | list[str],
        file_pattern: str | None = None,
        archive_path: str | None = None,
        force_repair: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Stub for data ingestion. Requires FILE skill."""
        raise NotImplementedError("ingest_to_lazyframe requires the FILE skill.")

    def discover_container_contents(
        self, archive_path: str, pattern: str | None = None
    ) -> list[str]:
        """Stub for archive discovery. Requires FILE skill."""
        raise NotImplementedError(
            "discover_container_contents requires the FILE skill."
        )

    def detect_encoding(
        self, fs: fsspec.AbstractFileSystem, target: str
    ) -> tuple[Any, str]:
        """Stub for encoding detection. Requires FILE skill."""
        raise NotImplementedError("detect_encoding requires the FILE skill.")

    def plan_distributed_batches(
        self, path: str, file_pattern: str | None = None
    ) -> list[list[str]]:
        """Stub for batch planning. Requires FILE skill."""
        raise NotImplementedError("plan_distributed_batches requires the FILE skill.")


class FileSystemSkills(Enum):
    """
    Registry of dynamically injectable Mixin classes.
    Used to compose specialized clients (e.g. ManagedS3Client with Ingestion).
    """

    FILE = ("file", "libs.file.mixins.data.FlatFileMixin")
    CAS = ("cas", "libs.file.mixins.cas.CASArchiveMixin")
    ARCHIVE = ("archive", "libs.file.mixins.archive.StandardArchiveMixin")

    def __init__(self, key: str, class_path: str):
        self.key = key
        self.class_path = class_path

    @property
    def mixin_class(self):
        """
        Dynamically imports and returns the Mixin class.

        Returns:
            type: The Python class object for the mixin.
        """
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
    Factory that assembles a Client with dynamic Mixin capabilities.

    Args:
        url: The root URL (e.g. s3://bucket).
        capabilities: A set of Skills (Mixins) to inject into the class.
        storage_options: Connection configuration.

    Returns:
        FileSystemClient: A dynamically generated client instance.
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
    for prefix, client_class in protocol_map.items():
        if url.startswith(prefix):
            base_class = cast("type[FileSystemClient]", client_class)
            break

    # 2. Collect Mixins from Enum
    # Decision: Mixin-First MRO.
    # We place Mixins BEFORE the base class so their implementations
    # take precedence over the interface stubs defined in FileSystemClient.
    bases = []
    for cap in capabilities:
        bases.append(cap.mixin_class)
    bases.append(base_class)

    # 3. Create Dynamic Managed Type
    class_name = f"Managed{base_class.__name__}"
    managed_class = type(class_name, tuple(bases), {})

    return managed_class(url=url, storage_options=storage_options)
