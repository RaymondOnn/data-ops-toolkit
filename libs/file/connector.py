import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any

from libs.file.archive import ArchiveContext
from libs.file.base import FileSystemSkills, create_fs_client
from libs.file.mixins.data import FileReader

LOG = logging.getLogger(__name__)
SKILL_REGISTRY: dict[FileSystemSkills, type] = {
    FileSystemSkills.FILE: FileReader,
    # FileSystemSkills.CAS: CASArchivalSkill,  # Plug in CAS skill when available
}


class FileSystemConnector:
    """
    Facade uniting file system I/O (via protocol-agnostic FileSystemClient)
    and high-performance file-based SQL engine (via DuckDB-backed DatabaseConnector).
    """

    def __init__(
        self,
        url: str,
        skills: set[FileSystemSkills] | FileSystemSkills | None = None,
        **conn_kwargs: Any,
    ) -> None:
        """
        Initializes the FileSystemConnector.

        Args:
            fs_client: Storage client for S3, Azure, or Local Filesystem.
            threads: DuckDB execution thread limit.
            max_memory: DuckDB memory ceiling.
        """
        self.fs = create_fs_client(url, set(), **conn_kwargs)

        if isinstance(skills, FileSystemSkills):
            self.active_skill_keys = {skills}
        else:
            self.active_skill_keys = skills or set()

        self.skills: dict[FileSystemSkills, Any] = {}
        for skill_key in self.active_skill_keys:
            if skill_key in SKILL_REGISTRY:
                strategy_cls = SKILL_REGISTRY[skill_key]
                # Pass self as connector context
                self.skills[skill_key] = strategy_cls(fs=self.fs)

    @property
    def data(self) -> FileReader:
        if not (data_processor := self.skills[FileSystemSkills.FILE]):
            raise ValueError("FileReader is required but initialised...")

        return data_processor

    def archive(
        self, archive_path: str, temp_dir: str | Path | None = None
    ) -> ArchiveContext:
        """Factory method to instantiate an ArchiveContext bound to this connector."""
        return ArchiveContext(
            fs_client=self, archive_path=archive_path, temp_dir=temp_dir
        )

    @staticmethod
    def is_archive(path: str) -> bool:
        """Checks if path matches known tape/tar archive extensions."""
        return ArchiveContext.is_archive(path)

    @staticmethod
    def is_zip(path: str) -> bool:
        """Checks if path matches known ZIP archive extensions."""
        return ArchiveContext.is_zip(path)

    @staticmethod
    def is_supported_archive(path: str) -> bool:
        """Checks if path is any supported archive or compressed virtual protocol."""
        return ArchiveContext.is_supported(path)

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
