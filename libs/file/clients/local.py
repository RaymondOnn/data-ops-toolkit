import logging
from typing import Any, Optional

import fsspec

from libs.file.base import FileSystemClient


LOG = logging.getLogger(__name__)

class LocalClient(FileSystemClient):
    """
    Local FileSystem Driver.
    Storage Options:
        - auto_mkdir: True (Automatically create parent directories)
    """
    def __init__(self, url: str, storage_options: Optional[dict[str, Any]] = None):
        super().__init__(url, storage_options)
        self.fs = self.connect()
        
    def connect(self) -> fsspec.LocalFileSystem:
        if not self.fs:
            self.fs = fsspec.filesystem("file", **self.opts)
        return self.fs
        