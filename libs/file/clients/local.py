import logging
from typing import Any

import fsspec
from fsspec.implementations.local import LocalFileSystem
from libs.file.base import FileSystemClient

LOG = logging.getLogger(__name__)


class LocalClient(FileSystemClient):
    """
    Local FileSystem Driver.
    Storage Options:
        - auto_mkdir: True (Automatically create parent directories)
    """

    def __init__(self, url: str, **options: Any) -> None:
        """Initialize Local client matching the factory signature."""
        super().__init__(url, **options)

    @property
    def fs(self) -> LocalFileSystem:
        """Concrete implementation of the abstract property from FileSystemClient."""
        if self._fs is None:
            self._fs = fsspec.filesystem("file", **self.options)
        return self._fs
