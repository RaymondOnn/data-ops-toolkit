import logging
from typing import Any

import fsspec
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

    def __init__(self, url: str, storage_options: dict[str, Any] | None = None) -> None:
        super().__init__(url, storage_options)

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        if not hasattr(self, "_fs") or self._fs is None:
            # Map generic 'password' to Azure-specific 'account_key'
            if "password" in self.options:
                self.options["account_key"] = self.options.pop("password")

            self._fs = fsspec.filesystem("abfs", **self.options)

        return self._fs
