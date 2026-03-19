import logging
from typing import Any, Optional

import fsspec
from adlfs import AzureBlobFileSystem

from libs.file.base import FileSystemClient


LOG = logging.getLogger(__name__)


class AzureClient(FileSystemClient):
    """
    Azure Blob Storage (ABFS) Driver.

    Storage Options:
        - account_name (str): Azure Storage Account Name
        - account_key (str): Azure Storage Access Key
        - connection_string (str): Full Azure connection string
    """

    def __init__(self, url: str, storage_options: Optional[dict[str, Any]] = None) -> None:
        super().__init__(url, storage_options)
        self.fs = self.connect()

    def connect(self) -> AzureBlobFileSystem:
        if not self.fs:
            # Map generic 'password' to Azure-specific 'account_key'
            if "password" in self.opts:
                self.opts["account_key"] = self.opts.pop("password")

            self.fs = fsspec.filesystem("abfs", **self.opts)

        return self.fs
