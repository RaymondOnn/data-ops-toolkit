import logging
from abc import ABC, abstractmethod
from typing import Any, Optional
from pathlib import Path
from enum import Enum

import fsspec

from libs.clients.base import BaseIOClient

LOG = logging.getLogger(__name__)

# TODO: Qurantine for file ingestion job
class FileSystemClient(BaseIOClient, ABC):
    def __init__(
        self, 
        url: str, 
        storage_options: Optional[dict[str, Any]] = None
    ):
        self.url = url.rstrip('/')
        self.opts = storage_options or {}
        self.fs = None

    @abstractmethod
    def connect(self) -> fsspec.AbstractFileSystem:
        raise NotImplementedError("Subclasses must implement connect() method.")

        
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
        if "://" in path and self.fs:
            stripped = self.fs._strip_protocol(path)
            # Normalize slashes and remove internal '.' or '..'
            normalized = "/".join([p for p in stripped.split("/") if p not in (".", "")])
            return str(self.fs.unstrip_protocol(normalized))
        return str(Path(path).resolve())

    def smart_transfer(self, local_source: str, remote_dest: str) -> None:
        """Handles local-to-cloud or cloud-to-cloud transfers safely."""
        if "://" in local_source and "://" in remote_dest:
            # Remote to Remote (e.g., S3 to S3)
            self.fs.cp(local_source, remote_dest)
        else:
            # Local to Remote (Upload)
            self.fs.put(local_source, remote_dest)

# We import these at the top level or inside the function
# To keep memory low, we can import them inside the Enum if needed
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
    capabilities: list[FileSystemSkills], 
    storage_options: Optional[dict[str, Any]] = None
) -> FileSystemClient:
    """
    Assembles a Managed Client with dynamic capabilities (Ingestion, Archive, etc.).
    """
    from .clients.s3 import S3Client
    from .clients.azure import AzureClient
    from .clients.local import LocalClient

    # 1. Map Protocol to Base Class
    protocol_map = {
        "s3://": S3Client,
        # "gs://": GCSClient,
        # "gcs://": GCSClient,
        "abfs://": AzureClient,
        "az://": AzureClient
    }
    
    # Default to LocalClient if no cloud protocol is detected
    base_class = LocalClient
    for prefix, cls in protocol_map.items():
        if url.startswith(prefix):
            base_class = cls
            break

    # 2. Collect Mixins from Enum
    bases = [base_class]
    for cap in capabilities:
        bases.append(cap.mixin_class)

    # 3. Create Dynamic Managed Type
    class_name = f"Managed{base_class.__name__}"
    ManagedClientClass = type(class_name, tuple(bases), {})

    # 4. Instantiate (triggers super().__init__ and fsspec setup)
    return ManagedClientClass(url=url, storage_options=storage_options)