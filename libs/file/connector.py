import logging
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from libs.file.clients.base import FileSystemClient
from libs.file.skills.base import FileSkill

if TYPE_CHECKING:
    from libs.file.skills.archive import ArchiveSkill
    from libs.file.skills.data import FileReader

LOG = logging.getLogger(__name__)


class FileSystemConnector:
    """
    Facade uniting file system I/O (via protocol-agnostic FileSystemClient)
    and high-performance file-based SQL engine (via DuckDB-backed DatabaseConnector).
    """

    def __init__(self, url: str, **conn_kwargs: Any) -> None:
        self.url = url
        self.conn_kwargs = conn_kwargs

        # Pure storage client initialization (lightweight & instant)
        self.fs = FileSystemClient.get_client(url, **conn_kwargs)
        self._skills: dict[str, FileSkill] = {}

    def skill(self, name: str) -> Any:
        """Lazily load and instantiate a skill bound to the underlying FileSystemClient."""
        key = name.lower()
        if key not in self._skills:
            skill_cls = FileSkill.get_class(key)
            # Instantiates skill with self.fs (NOT self)
            self._skills[key] = skill_cls(fs=self.fs, **self.conn_kwargs)
            LOG.debug(f"Loaded FileSkill '{key}' on demand.")
        return self._skills[key]

    @property
    def data(self) -> "FileReader":
        """Data streaming and DuckDB execution skill."""
        return self.skill("data")

    @property
    def archive(self) -> "ArchiveSkill":
        """Archive virtualization skill."""
        return self.skill("archive")

    # def archive(
    #     self, archive_path: str, temp_dir: str | Path | None = None
    # ) -> ArchiveContext:
    #     """Factory method to instantiate an ArchiveContext bound to this connector."""
    #     return ArchiveContext(
    #         fs_client=self, archive_path=archive_path, temp_dir=temp_dir
    #     )

    def is_archive(self, path: str) -> bool:
        """Checks if path matches known tape/tar archive extensions."""
        return self.archive.is_archive(path)

    def is_zip(self, path: str) -> bool:
        """Checks if path matches known ZIP archive extensions."""
        return self.archive.is_zip(path)

    def is_supported_archive(self, path: str) -> bool:
        """Checks if path is any supported archive or compressed virtual protocol."""
        return self.archive.is_supported(path)

    # =========================================================================
    # 1. FILE SYSTEM OPERATIONS (Delegated to FileSystemClient)
    # =========================================================================

    def resolve(self, path: str | Path) -> str:
        """Resolves path into absolute/fully-qualified string."""
        return self.fs.resolve(path)

    def exists(self, path: str | Path) -> bool:
        """Checks if file/path exists."""
        return self.fs.exists(path)

    def ls(self, path: str = "", detail: bool = False) -> list[Any]:
        """Lists directory contents."""
        return self.fs.ls(path, detail=detail)

    def glob(
        self,
        path: str | Path,
        pattern: str | None = None,
        recursive: bool = False,
        stream: bool = False,
    ) -> list[str] | Generator[str, None, None]:
        """Finds file paths matching a pattern."""
        return self.fs.glob(path, pattern=pattern, recursive=recursive, stream=stream)

    def cp(self, src: str, dst: str, recursive: bool = True, **kwargs) -> None:
        """Transfers files across filesystems (Upload, Download, or Cross-cloud)."""
        LOG.info(f"Transferring {src} -> {dst}")
        self.fs.cp(src, dst, recursive=recursive, **kwargs)

    def mv(self, src: str, dst: str, recursive: bool = True, **kwargs) -> None:
        """Moves files/directories."""
        self.fs.mv(src, dst, recursive=recursive, **kwargs)

    def rm(self, path: str, recursive: bool = False) -> None:
        """Removes a file or path."""
        LOG.info(f"Removing storage path: {path}")
        self.fs.rm(path, recursive=recursive)

    def is_file(self, path: str) -> bool | None:
        """
        Check if a path points to a file.

        Returns True for files, False for directories, and None if the path
        does not exist or the type cannot be determined.
        """
        return self.fs.fs.isfile(path)

    def find(
        self,
        path: str,
        max_depth: int | None = None,
        include_folders: bool = False,
    ) -> list[str]:
        """
        Find files using the underlying filesystem client.

        Args:
            path: The directory path to search within.
            max_depth: Maximum depth to search (None for unlimited).
            include_folders: Whether to include directories in the results.

        Returns:
            List of file paths matching the search criteria.
        """
        return list(self.fs.fs.find(path, maxdepth=max_depth, withdirs=include_folders))

    # =========================================================================
    # 2. DATA & SQL OPERATIONS (Delegated to DatabaseConnector / DuckDB)
    # =========================================================================

    # =========================================================================
    # 3. LIFECYCLE MANAGEMENT
    # =========================================================================

    def close(self) -> None:
        """Shuts down DuckDB engine and closes underlying fsspec connections."""
        self.data.db.close()
        self.fs.close()
        LOG.debug("FileSystemConnector closed successfully.")
