"""CAS (Content-Addressable Storage) archival with deduplication and immutability."""

import hashlib
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import fsspec
import polars as pl
from libs.file.formats import FormatFactory
from libs.file.utils import filter_files
from libs.utils.dates import current_timestamp
from upath import UPath

LOG = logging.getLogger(__name__)


def compute_sha256(file_path: str, chunk_size: int = 65536) -> str:
    """Compute SHA-256 hash of a file in chunks (memory efficient)."""
    sha256 = hashlib.sha256()
    with UPath(file_path).open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


class CASArchiveMixin:
    """
    Content-Addressable Storage for immutable, deduplicated archival.

    Architecture:
    - Physical Tier (Vault): Stores files by content hash (SHA-256)
    - Logical Tier (Manifests): JSON pointers for job/date lookup

    Path Structure:
    - Vault: archive/vault/{hash[:2]}/{hash[2:4]}/{hash}/{filename}
    - Manifest: archive/jobs/{job_id}/{YYYY}/{MM}/{DD}/manifest.json
    """

    fs: fsspec.AbstractFileSystem
    url: str
    options: dict[str, Any]

    def resolve(self, path: str) -> str:
        raise NotImplementedError

    def is_archive(self, path: str) -> bool:
        raise NotImplementedError

    def extract_archive(self, path: str) -> str:
        raise NotImplementedError

    # =========================================================================
    # Public API
    # =========================================================================

    def archive_to_cas(
        self,
        local_path: str,
        job_id: str,
        metadata: dict[str, Any] | None = None,
        reference_date: datetime | None = None,
    ) -> tuple[str, str]:
        """
        Archive a file to CAS vault with deduplication.

        Args:
            local_path: Path to local file to archive.
            job_id: Job identifier for the manifest.
            metadata: Optional metadata to store.
            reference_date: Date for backfill (default: now).

        Returns:
            Tuple of (vault_path, manifest_path)
        """
        # 1. Compute hash and sharded path
        file_hash = compute_sha256(local_path)
        vault_path = self._get_vault_path(file_hash, local_path)
        vault_dir = str(vault_path.parent)

        # 2. Upload to vault (idempotent - skips if exists)
        if not self.fs.exists(str(vault_path)):
            self._atomic_upload(local_path, str(vault_path), vault_dir)
        else:
            LOG.info(f"CAS: Content already exists (hash={file_hash[:8]}...)")

        # 3. Write manifest pointer
        manifest_path = self._write_manifest(
            job_id, file_hash, str(vault_path), metadata, reference_date
        )

        return str(vault_path), manifest_path

    def retrieve(self, job_id: str, date_str: str) -> str:
        """
        Retrieve physical vault path for a job run.

        Args:
            job_id: Job identifier.
            date_str: Date in 'YYYY/MM/DD' format.

        Returns:
            Physical path in vault.

        Raises:
            FileNotFoundError: If manifest or file missing.
        """
        manifest_path = f"{self.url}/archive/jobs/{job_id}/{date_str}/manifest.json"
        temp_dir = None

        try:
            # Check if manifest path is in an archive
            if self.is_archive(manifest_path):
                temp_dir = self.extract_archive(manifest_path)
                manifest_path = temp_dir + "/manifest.json"

            if not self.fs.exists(manifest_path):
                raise FileNotFoundError(
                    f"No manifest found for job {job_id} on {date_str}"
                )

            with self.fs.open(manifest_path, "rb") as f:
                manifest = json.load(f)

            physical_path = manifest["physical_path"]

            # Check if physical_path is in an archive
            if self.is_archive(physical_path):
                temp_dir2 = self.extract_archive(physical_path)
                physical_path = temp_dir2 + "/" + Path(physical_path).name

            if not self.fs.exists(physical_path):
                raise FileNotFoundError(f"Physical file missing: {physical_path}")

            return str(physical_path)

        finally:
            if temp_dir:
                shutil.rmtree(temp_dir, ignore_errors=True)

    def scan_manifests(self, job_id: str | None = None) -> pl.DataFrame:
        """
        Scan all manifests into a Polars DataFrame for audit.

        Args:
            job_id: Optional job filter.

        Returns:
            DataFrame with manifest data.
        """
        search_path = f"{self.url}/archive/jobs/{job_id if job_id else '**'}"
        temp_dir = None

        try:
            # Check if search_path is an archive
            if self.is_archive(search_path):
                temp_dir = self.extract_archive(search_path)
                search_path = temp_dir

            # Standard directory search
            resolved = self.resolve(search_path)
            all_files = (
                self.fs.find(resolved) if self.fs.isdir(resolved) else [resolved]
            )
            targets = filter_files(all_files, "manifest.json", search_path)

            if not targets:
                return pl.DataFrame()

            # Load manifests
            data = []
            for t in targets:
                with self.fs.open(t, "rb") as f:
                    data.append(json.load(f))

            return pl.DataFrame(data)

        finally:
            if temp_dir:
                shutil.rmtree(temp_dir, ignore_errors=True)

    def garbage_collect(self, dry_run: bool = True) -> int | None:
        """
        Remove orphaned vault files not referenced by any manifest.

        Args:
            dry_run: If True, only log what would be deleted.

        Returns:
            Number of files deleted (or None if no manifests found).
        """
        LOG.info(f"Starting garbage collection (dry_run={dry_run})")

        # Get all referenced hashes from manifests
        manifest_df = self.scan_manifests()
        if manifest_df.is_empty():
            LOG.warning("No manifests found - aborting to prevent data loss")
            return None

        active_hashes = set(manifest_df["content_hash"].to_list())

        # Find all files in vault
        vault_root = f"{self.url}/archive/vault/"
        vault_files = self.fs.find(vault_root)

        # Identify orphans
        orphans = []
        for file_path in vault_files:
            # Extract hash from path: .../vault/aa/bb/full_hash/filename
            parts = file_path.split("/")
            if len(parts) >= 3:
                file_hash = parts[-2]  # parent directory is hash
                if file_hash not in active_hashes:
                    orphans.append(file_path)

        # Delete orphans
        for path in orphans:
            if dry_run:
                LOG.info(f"[DRY RUN] Would delete: {path}")
            else:
                LOG.warning(f"Deleting orphan: {path}")
                self.fs.rm(path)

        LOG.info(f"Found {len(orphans)} orphaned files")
        return len(orphans)

    # =========================================================================
    # Internal Helpers
    # =========================================================================

    def _get_vault_path(self, file_hash: str, original_path: str) -> UPath:
        """Get vault path for a file hash."""
        base: UPath = UPath(self.url, **self.options)
        filename = Path(original_path).name
        return (
            base
            / "archive"
            / "vault"
            / file_hash[:2]
            / file_hash[2:4]
            / file_hash
            / filename
        )

    def _atomic_upload(self, local_path: str, vault_path: str, vault_dir: str) -> None:
        """Atomic upload with temporary file."""
        temp_path = f"{vault_path}.tmp"

        self.fs.makedirs(vault_dir, exist_ok=True)
        LOG.debug(f"CAS: Uploading to {temp_path}")

        self.fs.put(local_path, temp_path)
        self.fs.mv(temp_path, vault_path)

        LOG.info(f"CAS: Stored at {vault_path}")

    def _write_manifest(
        self,
        job_id: str,
        file_hash: str,
        vault_path: str,
        metadata: dict[str, Any] | None,
        reference_date: datetime | None,
    ) -> str:
        """Write logical manifest pointer."""
        target_date = reference_date or current_timestamp()
        base = UPath(self.url, **self.options)

        manifest_dir = (
            base
            / "archive"
            / "jobs"
            / job_id
            / target_date.strftime("%Y")
            / target_date.strftime("%m")
            / target_date.strftime("%d")
        )
        manifest_path = manifest_dir / "manifest.json"

        manifest = {
            "job_id": job_id,
            "logical_date": target_date.strftime("%Y/%m/%d"),
            "processed_at": current_timestamp(naive=True).isoformat(sep=" "),
            "content_hash": file_hash,
            "physical_path": vault_path,
            "metadata": metadata or {},
        }

        self.fs.makedirs(str(manifest_dir), exist_ok=True)

        handler = FormatFactory.get("json", self.fs, self.options)
        handler.write_raw(
            json.dumps(manifest, indent=4).encode("utf-8"), str(manifest_path)
        )

        return str(manifest_path)
