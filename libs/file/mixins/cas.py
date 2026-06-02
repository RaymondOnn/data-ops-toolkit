import hashlib
import json
import logging
from datetime import datetime  # Keep for type hinting reference_date
from pathlib import Path
from typing import Any

import fsspec
import polars as pl
from libs.file.formats import FormatFactory
from libs.file.utils import filter_files
from libs.utils.dates import get_current_timestamp
from upath import UPath

LOG = logging.getLogger(__name__)


def calculate_sha256(local_path: str) -> str:
    """
    Calculates the SHA-256 hash of a local file in chunks.

    Args:
        local_path: The filesystem path to the local file.

    Returns:
        str: The hex digest of the file's content.
    """
    sha256_hash = hashlib.sha256()
    with UPath(local_path).open("rb") as f:
        for byte_block in iter(lambda: f.read(65536), b""):  # 64KB chunks
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


class CASArchiveMixin:
    """
    Approach: Content-Addressable Storage (CAS) with Logical Manifests

    This Mixin implements a two-tiered storage architecture designed for
    idempotency, deduplication, and high-volume data lineage.

    1. Physical Tier (The Vault):
    - Files are stored based on their SHA-256 content hash.
    - Path structure: `archive/vault/{hash_prefix_1}/{hash_prefix_2}/{full_hash}/{filename}`
    - Deduplication: If multiple jobs ingest the same file, it is only stored once.
    - Atomic Commits: Files are uploaded with a .tmp extension and moved to their
        final destination only upon successful transfer to prevent corruption.

    2. Logical Tier (The Manifests):
    - Small JSON files that act as pointers to the Physical Tier.
    - Path structure: `archive/jobs/{job_id}/{YYYY}/{MM}/{DD}/manifest.json`
    - Idempotency: Rerunning a job on the same day overwrites the manifest but
        links to the same (or updated) hash in the vault.
    - Searchability: Allows O(1) retrieval of data by job and date without
        scanning millions of physical files.

    3. Maintenance:
    - Crawling: Aggregates logical manifests into Polars DataFrames for audit trails.
    - Garbage Collection: Safely removes vault files not referenced by any manifest.
    """

    fs: fsspec.AbstractFileSystem
    url: str
    opts: dict[str, Any]

    def archive_to_cas(
        self, local_path: str, job_id: str, metadata: dict[str, Any] | None = None
    ) -> tuple[str, str]:
        """Archives a local file to the Content-Addressable Storage vault.

        Generates a content hash, stores the file in a sharded physical vault,
        and creates a logical manifest pointer for the specific job.

        Args:
            local_path: Path to the local file to archive.
            job_id: Identifier for the job creating the archive.
            metadata: Optional dictionary of metadata to store in the manifest.

        Returns:
            tuple[str, str]: (generated_vault_path, logical_manifest_path).

        Decision: Sharded Entropy.
        We use the first 4 characters of the hash to create a two-tier directory
        structure. This prevents any single directory from exceeding OS-level
        file count limits when the vault grows to millions of artifacts.
        """
        """
        Archives a local file to the Content-Addressable Storage vault.

        Generates a content hash, stores the file in a sharded physical vault,
        and creates a logical manifest pointer for the specific job.

        Args:
            local_path: Path to the local file to archive.
            job_id: Identifier for the job creating the archive.
            metadata: Optional dictionary of metadata to store in the manifest.

        Returns:
            tuple[str, str]: A tuple containing the generated vault path
                and the manifest path.
        """

        # 1. Generate Hash and Sharded Path
        file_hash = calculate_sha256(local_path)
        base = UPath(self.url, **self.opts)
        vault_dir = (
            base / "archive" / "vault" / file_hash[:2] / file_hash[2:4] / file_hash
        )

        # Keep original filename inside the hash-folder
        vault_path = vault_dir / Path(local_path).name

        # 2. Idempotent Vault Storage (Physical Tier)
        if not self.fs.exists(str(vault_path)):
            self._atomic_vault_upload(local_path, str(vault_path), str(vault_dir))
        else:
            LOG.info(
                "CAS: Content already exists in vault, skipping upload.",
                extra={"hash": file_hash},
            )

        # 3. Idempotent Manifest Write (Logical Tier)
        manifest_path = self._write_cas_manifest(
            job_id, file_hash, str(vault_path), metadata
        )

        return str(vault_path), str(manifest_path)

    def _atomic_vault_upload(
        self, local_path: str, vault_path: str, vault_dir: str
    ) -> None:
        """
        Ensures file integrity by using a temporary upload path.

        Args:
            local_path: Source path on the local filesystem.
            vault_path: Final destination path in the CAS vault.
            vault_dir: The sharded directory containing the vault file.
        """
        temp_path = f"{vault_path}.tmp"

        self.fs.makedirs(vault_dir, exist_ok=True)
        LOG.debug(f"CAS: Uploading to temporary path {temp_path}")

        self.fs.put(local_path, temp_path)

        # Rename is atomic in S3 and local filesystems
        self.fs.mv(temp_path, vault_path)
        LOG.info(f"CAS: Atomic commit complete for {vault_path}")

    def _write_cas_manifest(
        self,
        job_id: str,
        file_hash: str,
        vault_path: str,
        meta: dict[str, Any] | None,
        reference_date: datetime | None = None,  # New: Support for backfills
    ) -> str:
        """
        Writes the logical pointer (manifest) for a CAS entry.

        Args:
            job_id: The unique identifier for the job.
            file_hash: The SHA-256 hash of the content.
            vault_path: The physical path where the content is stored.
            meta: Metadata dictionary to persist with the manifest.
            reference_date: Optional date for backfills. Defaults to now.

        Returns:
            str: The full path to the created manifest JSON file.
        """
        # Use the provided date (backfill) or current date (standard run)
        target_date = reference_date or get_current_timestamp()

        base = UPath(self.url, **self.opts)
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

        manifest_data = {
            "job_id": job_id,
            "logical_date": target_date.strftime("%Y/%m/%d"),
            "processed_at": get_current_timestamp(strip_tz=True).isoformat(sep=" "),
            "content_hash": file_hash,
            "physical_path": vault_path,
            "metadata": meta or {},
        }

        self.fs.makedirs(str(manifest_dir), exist_ok=True)

        # Write using JSONHandler
        handler = FormatFactory.get_handler("json", self.fs, self.opts)
        handler.write(
            json.dumps(manifest_data, indent=4).encode("utf-8"), str(manifest_path)
        )

        return str(manifest_path)

    def crawl_manifests(self, job_id: str | None = None) -> pl.DataFrame:
        """
        Aggregates logical manifests into a single Polars DataFrame.

        Args:
            job_id: If provided, limits the crawl to a specific job's history.
                Otherwise, crawls all manifests across the system.

        Returns:
            pl.DataFrame: A DataFrame containing the combined manifest data
                useful for audit trails and lineage reporting.

        Decision: JSONL over JSON.
        While manifests are single JSON files, we recommend JSONL for
        high-volume logging. For crawling, we load manifests into Polars
        DataFrames because manifest volume can grow to 100k+ files, which
        Polars handles much more efficiently than standard Python lists.
        """
        """Aggregates logical manifests into a single Polars DataFrame.

        Decision: Container Agnostic Discovery.
        We use _mount_archive_fs to handle cases where the entire job
        history might be encapsulated in an archive (e.g. historical_jobs.zip).
        This allows the crawler to see manifest.json files regardless of
        the physical storage medium.
        """
        search_path = f"{self.url}/archive/jobs/{job_id if job_id else '**'}"
        fs = self._mount_archive_fs(search_path)

        # If the mount returns a virtual FS (archive), we use container discovery
        if fs is not self.fs:
            targets = self.get_archive_contents(search_path, pattern="manifest.json")
        else:
            # Standard directory search
            resolved = self.resolve_path(search_path)
            all_files = fs.find(resolved) if fs.isdir(resolved) else [resolved]
            targets = filter_files(all_files, "manifest.json", search_path)

        if not targets:
            return pl.DataFrame()

        # We can use read_json on the collected list for a quick report
        # Since manifests are small, this is safe for memory.
        lfs = [pl.read_json(self.fs.open(t, "rb").read()) for t in targets]
        return pl.concat([df.lazy() for df in lfs]).collect()

    def retrieve_file(self, job_id: str, date_str: str) -> str:
        """
        Finds the physical vault path for a specific job run.

        Args:
            job_id: The job identifier.
            date_str: Date string in 'YYYY/MM/DD' format.

        Returns:
            str: The resolved physical path in the vault.

        Raises:
            FileNotFoundError: If the manifest or the physical file is missing.
        """
        manifest_path = f"{self.url}/archive/jobs/{job_id}/{date_str}/manifest.json"

        if not self.fs.exists(manifest_path):
            raise FileNotFoundError(f"No manifest found for job {job_id} on {date_str}")

        with self.fs.open(manifest_path, "rb") as f:
            manifest = json.load(f)

        physical_path = manifest["physical_path"]

        if not self.fs.exists(physical_path):
            raise FileNotFoundError(
                f"Manifest exists, but physical file is missing: {physical_path}"
            )

        return str(physical_path)

    def garbage_collect(self, dry_run: bool = True) -> None | int:
        """
        Removes orphaned files in the vault not referenced by any manifest.

        Args:
            dry_run: If True, logs intended deletions without removing files.

        Returns:
            Optional[int]: The number of files deleted (or marked for deletion).
                Returns None if no manifests are found to prevent total loss.
        """
        LOG.info(f"🧹 Starting Garbage Collection (Dry Run: {dry_run})")

        # 1. Get all manifest-referenced hashes
        manifest_df = self.crawl_manifests()
        if manifest_df.is_empty():
            LOG.warning("No manifests found. Aborting GC to prevent total data loss.")
            return None

        active_hashes = set(manifest_df["content_hash"].to_list())

        # 2. List all files physically present in the vault
        vault_files = self.fs.find(f"{self.url}/archive/vault/")

        # 3. Identify orphans
        to_delete = []
        for file_path in vault_files:
            # Extract hash from path (it's the parent folder name in our shard logic)
            # path/to/vault/aa/bb/full_hash/filename.csv -> parent of filename is full_hash
            path_parts = file_path.split("/")
            file_hash = path_parts[-2]  # Based on our vault structure

            if file_hash not in active_hashes:
                to_delete.append(file_path)

        # 4. Execute
        for path in to_delete:
            if dry_run:
                LOG.info(f"[DRY RUN] Would delete orphaned file: {path}")
            else:
                LOG.warning(f"Deleting orphaned file: {path}")
                self.fs.rm(path)

        return len(to_delete)
