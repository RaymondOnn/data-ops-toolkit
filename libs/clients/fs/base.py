import logging
from abc import ABC
from io import BytesIO
from typing import Any, Optional
from datetime import datetime
from pathlib import Path

import fsspec
import polars as pl

from .utils import repair_json, repair_xml # Moved your repair functions here
from libs.clients.base import BaseIOClient

LOG = logging.getLogger(__name__)


class FSClient(BaseIOClient, ABC):
    def __init__(
        self, 
        url: str, 
        storage_options: Optional[dict[str, Any]] = None
    ):
        self.url = url.rstrip('/')
        self.opts = storage_options or {}
        # Bridged to structlog via the wrapper function we discussed
        
        protocol = self.url.split("://")[0] if "://" in self.url else "file"
        self.fs = fsspec.filesystem(protocol, **self.opts)
        
    # ?: Perhaps quarantine file as a separate method    
    # def validate_integrity(self, path: str) -> bool:
    #     """Self-healing: Checks for 0-byte files and moves them to quarantine."""
    #     full_path = self._get_full_path(path)
    #     if not self.fs.exists(full_path):
    #         return False
        
    #     if self.fs.size(full_path) == 0:
    #         LOG.error("Zero-byte file detected", extra={"path": full_path, "event": "quarantine"})
    #         quarantine_path = f"{self.url}/quarantine/{path.split('/')[-1]}"
    #         self.fs.makedirs(self.fs._parent(quarantine_path), exist_ok=True)
    #         self.fs.move(full_path, quarantine_path)
    #         return False
    #     return True
    
    def validate_before_read(self, fs: fsspec.AbstractFileSystem, target: str) -> bool:
        """Step 2.5: Sanity checks. Returns False if file is invalid."""
        if not fs.exists(target):
            LOG.error(f"Source file missing: {target}")
            return False
        
        size = fs.size(target)
        if size == 0:
            LOG.error(f"Zero-byte file detected: {target}")
            # Self-healing logic (quarantine) could be called here
            return False
            
        if size > 5 * 1024**3:
            LOG.warning(f"Very large file (>5GB): {target}. Forcing streaming mode.")
        
        return True
    
    
    def resolve_path(self, path: str) -> str:
        """The fsspec equivalent of Path.resolve()."""
        if "://" in path:
            stripped = self.fs._strip_protocol(path)
            # Normalize slashes and remove internal '.' or '..'
            normalized = "/".join([p for p in stripped.split("/") if p not in (".", "")])
            return str(self.fs.unstrip_protocol(normalized))
        return str(Path(path).resolve())


    
        
    class FileArchiveMixin:
        def archive_to_cas(self, local_path: str, metadata: dict) -> str:
            """Moves a local file into the sharded CAS structure."""
            from .utils import calculate_sha256
            
            file_hash = calculate_sha256(local_path)
            cas_path = f"{self.url}/archive/{file_hash[:2]}/{file_hash[2:4]}/{file_hash}"
            
            # 3. Check for existence (Deduplication)
            if not self.fs.exists(cas_path):
                self.fs.makedirs(self.fs._parent(cas_path), exist_ok=True)
                self.fs.put(local_path, cas_path)
                self._write_cas_manifest(file_hash, metadata)
            
            return file_hash

        def _write_cas_manifest(self, file_hash: str, meta: dict):
            manifest_path = f"{self.url}/archive/cas_registry.jsonl"
            entry = {"hash": file_hash, "ts": datetime.now().isoformat(), "meta": meta}
            with self.fs.open(manifest_path, "a") as f:
                f.write(json.dumps(entry) + "\n")