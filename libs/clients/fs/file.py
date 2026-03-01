import hashlib
import io
import json
import logging
import re
from datetime import datetime

import fsspec
import polars as pl
from charset_normalizer import from_bytes

from libs.clients.base import BaseIOClient


class FileClient(BaseIOClient):
    def __init__(self, url, file_pattern=None, storage_options=None):
        self.url = url  # This is the "Base" path or folder
        self.file_pattern = file_pattern
        self.opts = storage_options or {}
        self.fs = None
        self._active_handle = None
        self.url = url
        self.logger = logging.getLogger("FileDefense")

    def open(self):
        """Initializes the filesystem (S3, SFTP, or Local)"""
        # fsspec.url_to_fs splits "s3://bucket/path" into ("s3", "bucket/path")
        protocol = self.url.split("://")[0] if "://" in self.url else "file"
        self.fs = fsspec.filesystem(protocol, **self.opts)
        return self

    def fetch_by_hash(self, file_hash):
        """Retrieves a file from the CAS archive using its hash."""
        path = f"{self.url.rstrip('/')}/{file_hash[:2]}/{file_hash[2:4]}/{file_hash}"
        if not self.fs.exists(path):
            raise FileNotFoundError(f"Hash {file_hash} not found in archive.")

        # Returns a stream just like our previous 'fetch' methods
        return self.fs.open(path, mode="rb")

    def _pre_check(self):
        """Tier 1 Defense: Infrastructure level."""
        if not self.fs.exists(self.url):
            raise FileNotFoundError(f"Source file missing: {self.url}")

        size = self.fs.size(self.url)
        if size == 0:
            raise ValueError(f"Zero-byte file detected: {self.url}")

        # Limit check: Prevent OOM by flagging files > 5GB for special handling
        if size > 5 * 1024**3:
            self.logger.warning(
                f"Large file detected ({size} bytes). Switching to streaming mode."
            )

    def fetch(self, format_type=None):
        self._pre_check()
        fmt = format_type or self.url.split(".")[-1].lower()

        # 1. Open raw binary stream
        raw_stream = self.fs.open(self.url, mode="rb")

        try:
            # 2. Attempt "Self-Healing" repairs based on format
            if fmt == "xml":
                repaired_stream = self._repair_xml(raw_stream)
                return pl.read_xml(repaired_stream)  # Now safe to read

            elif fmt == "csv":
                repaired_stream = self._repair_csv(raw_stream)
                return pl.read_csv(repaired_stream, infer_schema_length=0)

            elif fmt == "json":
                repaired_stream = self._repair_json(raw_stream)
                return pl.read_json(repaired_stream)

        except Exception as e:
            self.logger.warning(f"Repair failed for {self.url}. Moving to quarantine.")
            self.quarantine(reason=str(e))
            raise

    # --- REPAIR LOGIC ---

    def _repair_xml(self, stream):
        """Removes ASCII control characters (0-31) except for tab, newline, carriage return."""
        self.logger.info(f"🛠️ Sanitizing XML: {self.url}")
        content = stream.read().decode("utf-8", errors="ignore")
        # Regex to keep only valid XML characters
        # Valid: #x9 | #xA | #xD | [#x20-#xD7FF] ...
        clean_content = re.sub(r"[^\x09\x0A\x0D\x20-\x7E]+", "", content)
        return io.BytesIO(clean_content.encode("utf-8"))

    def _repair_csv(self, stream):
        """Fixes common 'Excel-exported' CSV issues like the UTF-8 BOM."""
        content = stream.read()
        # Remove UTF-8 BOM if present (\xef\xbb\xbf)
        if content.startswith(b"\xef\xbb\xbf"):
            self.logger.info(f"🛠️ Stripping BOM from CSV: {self.url}")
            content = content[3:]
        return io.BytesIO(content)

    def _repair_json(self, stream):
        """Handles common 'Trailing Comma' issues in JSON arrays/objects."""
        content = stream.read().decode("utf-8")
        # Very basic regex to fix [1, 2, 3,] -> [1, 2, 3]
        clean_content = re.sub(r",\s*([\]}])", r"\1", content)
        return io.BytesIO(clean_content.encode("utf-8"))

    def load(self, local_path, remote_name):
        """The 'FileService' logic: Uploads/Archivers a file."""
        target_path = f"{self.url.rstrip('/')}/{remote_name}"
        self.fs.put(local_path, target_path)
        print(f"✅ Uploaded {local_path} to {target_path}")

    def archive_cas(self, local_path, metadata=None):
        """
        Archives a file using Content Addressable Storage (CAS).
        Works on S3, Azure, GCS, or Local via self.fs.
        """
        # 1. Generate the hash (The Content Address)
        file_hash = self._calculate_sha256(local_path)

        # 2. Create a partitioned path: /base/af/1c/af1c...
        # This keeps directories small and performant across all cloud providers
        hash_prefix = file_hash[:2]
        hash_subprefix = file_hash[2:4]

        # self.url is the base archive path (e.g., 's3://my-archive')
        remote_path = (
            f"{self.url.rstrip('/')}/{hash_prefix}/{hash_subprefix}/{file_hash}"
        )

        # 3. Check for existence (Deduplication)
        if not self.fs.exists(remote_path):
            self.logger.info(f"Storing new unique file: {file_hash}")
            self.fs.put(local_path, remote_path)

            # 4. Optional: Store metadata as a sidecar file if cloud-native tags aren't enough
            if metadata:
                self._write_sidecar_metadata(remote_path, metadata)
        else:
            self.logger.info(f"Deduplication triggered: {file_hash} already exists.")

        return file_hash

    def _calculate_sha256(self, local_path):
        sha256_hash = hashlib.sha256()
        with open(local_path, "rb") as f:
            for byte_block in iter(lambda: f.read(65536), b""):  # 64KB chunks
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _write_sidecar_metadata(self, remote_path, metadata):
        import json

        meta_path = f"{remote_path}.meta.json"
        with self.fs.open(meta_path, "w") as f:
            json.dump(metadata, f)

    def close(self):
        if self._active_handle:
            self._active_handle.close()

    def _get_safe_stream(self, raw_stream):
        """
        Peeks at the start of a binary stream to infer encoding
        and returns a wrapper or a decoded buffer.
        """
        # 1. Read a sample (usually 16KB-32KB is plenty)
        sample = raw_stream.read(32768)

        # 2. Detect encoding
        results = from_bytes(sample)
        best_match = results.best()

        # Fallback to utf-8 if detection is uncertain
        encoding = best_match.encoding if best_match else "utf-8"
        self.logger.info(
            f"🔍 Detected encoding: {encoding} (Confidence: {best_match.rating if best_match else 'N/A'})"
        )

        # 3. Handle the "Reset"
        # Since we read the sample, we must 'rewind' the stream for the reader
        try:
            raw_stream.seek(0)
            return raw_stream, encoding
        except (AttributeError, io.UnsupportedOperation):
            # If the stream doesn't support seek (like some SFTP streams),
            # we must prepend the sample back to the stream
            return io.BytesIO(sample + raw_stream.read()), encoding

    def fetch_as_polars(self):
        raw_stream = self.fs.open(self.url, mode="rb")
        stream, detected_enc = self._get_safe_stream(raw_stream)

        # Now Polars knows exactly how to decode the bytes
        return pl.read_csv(stream, encoding=detected_enc)

    def quarantine(self, reason):
        """
        Moves the offending file to a 'quarantine' bucket/folder
        and writes a JSON report.
        """
        quarantine_base = "s3://my-platform-data/quarantine"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.url.split("/")[-1]

        dest_path = f"{quarantine_base}/{timestamp}_{filename}"
        meta_path = f"{dest_path}.error.json"

        # 1. Move the corrupted file
        self.logger.error(f"☣️ Quarantining {self.url} to {dest_path}")
        self.fs.mv(self.url, dest_path)

        # 2. Write the 'Death Certificate'
        error_report = {
            "source_url": self.url,
            "error_reason": reason,
            "timestamp": timestamp,
            "file_size": self.fs.size(dest_path) if self.fs.exists(dest_path) else 0,
        }

        # Use fsspec to write the metadata directly to S3/SFTP
        with self.fs.open(meta_path, "w") as f:
            json.dump(error_report, f, indent=4)

    def update_manifest(self, file_hash, original_meta):
        """
        Maintains a 'manifest.jsonl' in the root of the archive.
        JSONL (JSON Lines) is better for appending than standard JSON.
        """
        manifest_path = f"{self.url.rstrip('/')}/ingestion_manifest.jsonl"

        entry = {
            "hash": file_hash,
            "timestamp": datetime.now().isoformat(),
            "original_name": original_meta.get("filename"),
            "size": original_meta.get("size"),
            "source": original_meta.get("source_system"),
        }

        # Append to the manifest file (fsspec handles this across S3/Azure/Local)
        with self.fs.open(manifest_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def garbage_collect(self, dry_run=True):
        """
        Compares the physical files in storage vs. the manifest.
        Deletes files that aren't registered (orphaned files).
        """
        self.logger.info("🧹 Starting Garbage Collection...")
        # 1. Load all hashes from manifest
        # 2. Crawl the filesystem for physical files
        # 3. If file exists but hash is not in manifest -> delete
        pass


def rebuild_metadata_from_manifest(archive_url, db_connection):
    fs = fsspec.filesystem(archive_url.split("://")[0])
    manifest_path = f"{archive_url}/ingestion_manifest.jsonl"

    with fs.open(manifest_path, "r") as f:
        for line in f:
            record = json.loads(line)
            # Sync back to SQL
            db_connection.execute(
                "INSERT INTO registry (hash, name) VALUES (?, ?)",
                (record["hash"], record["original_name"]),
            )
    print("✅ Metadata reconstructed successfully.")
