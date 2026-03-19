# from io import BytesIO
# import io
# import json
# from typing import Any, Literal
# import logging

# import re
# from datetime import datetime

# import fsspec
# import polars as pl
# from charset_normalizer import from_bytes

# from libs.clients.base import BaseIOClient

# LOG = logging.getLogger(__name__)


# def repair_json(stream: BytesIO) -> BytesIO:
#     """Handles common 'Trailing Comma' issues in JSON arrays/objects."""
#     content = stream.read().decode("utf-8")
#     # Very basic regex to fix [1, 2, 3,] -> [1, 2, 3]
#     clean_content = re.sub(r",\s*([\]}])", r"\1", content)
#     return BytesIO(clean_content.encode("utf-8"))

# def repair_xml(stream: BytesIO) -> BytesIO:
#     """Removes ASCII control characters (0-31) except for tab, newline, carriage return."""
#     LOG.info(f"🛠️ Sanitizing XML: {self.url}")
#     content = stream.read().decode("utf-8", errors="ignore")
#     # Regex to keep only valid XML characters
#     # Valid: #x9 | #xA | #xD | [#x20-#xD7FF] ...
#     clean_content = re.sub(r"[^\x09\x0A\x0D\x20-\x7E]+", "", content)
#     return BytesIO(clean_content.encode("utf-8"))

# def repair_csv(stream: BytesIO) -> BytesIO:
#     """Fixes common 'Excel-exported' CSV issues like the UTF-8 BOM."""
#     content = stream.read()
#     # Remove UTF-8 BOM if present (\xef\xbb\xbf)
#     if content.startswith(b"\xef\xbb\xbf"):
#         LOG.info(f"🛠️ Stripping BOM from CSV...")
#         content = content[3:]
#     return BytesIO(content)

# class FileClient(BaseIOClient):
#     def __init__(
#         self,
#         url: str,
#         file_pattern: str | None=None,
#         storage_options: dict[str, Any] | None=None
#     ) -> None:
#         self.url = url  # This is the "Base" path or folder
#         self.file_pattern = file_pattern
#         self.opts = storage_options or {}

#         protocol = self.url.split("://")[0] if "://" in self.url else "file"
#         self.fs = fsspec.filesystem(protocol, **self.opts)


#     def fetch_by_hash(self, file_hash: str) -> BytesIO:
#         """Retrieves a file from the CAS archive using its hash."""
#         path = f"{self.url.rstrip('/')}/{file_hash[:2]}/{file_hash[2:4]}/{file_hash}"
#         if not self.fs.exists(path):
#             raise FileNotFoundError(f"Hash {file_hash} not found in archive.")

#         # Returns a stream just like our previous 'fetch' methods
#         return self.fs.open(path, mode="rb")

#     # def _pre_check(self) -> None:
#     #     """Tier 1 Defense: Infrastructure level."""
#     #     if not self.fs.exists(self.url):
#     #         raise FileNotFoundError(f"Source file missing: {self.url}")

#     #     # Zero Byte Check
#     #     size = self.fs.size(self.url)
#     #     if size == 0:
#     #         raise ValueError(f"Zero-byte file detected: {self.url}")

#     #     # Limit check: Prevent OOM by flagging files > 5GB for special handling
#     #     if size > 5 * 1024**3:
#     #         LOG.warning(
#     #             f"Large file detected ({size} bytes). Switching to streaming mode."
#     #         )

#     # def fetch(self, format_type: str) -> Any | None:
#     #     self._pre_check()
#     #     fmt = format_type or self.url.split(".")[-1].lower()

#     #     if fmt not in ["xml", "csv", "json"]:
#     #         raise ValueError(f"Unsupported file format: {fmt}")

#     #     # 1. Open raw binary stream
#     #     raw_stream = self.fs.open(self.url, mode="rb")

#     #     try:
#     #         # 2. Attempt "Self-Healing" repairs based on format
#     #         if fmt == "xml":
#     #             repaired_stream = repair_xml(raw_stream)
#     #             return pl.read_xml(repaired_stream)  # Now safe to read

#     #         elif fmt == "csv":
#     #             repaired_stream = repair_csv(raw_stream)
#     #             return pl.read_csv(repaired_stream, infer_schema_length=0)

#     #         elif fmt == "json":
#     #             repaired_stream = repair_json(raw_stream)
#     #             return pl.read_json(repaired_stream)

#     #     except Exception as e:
#     #         LOG.warning(f"Repair failed for {self.url}. Moving to quarantine.")
#     #         self.quarantine(reason=str(e))
#     #         raise
#     #     finally:
#     #         raw_stream.close()

#     #     return None


#     # --- REPAIR LOGIC ---


#     def upload(self, local_path: str, remote_name: str) -> None:
#         """The 'FileService' logic: Uploads/Archivers a file."""
#         target_path = f"{self.url.rstrip('/')}/{remote_name}"
#         self.fs.put(local_path, target_path)
#         print(f"✅ Uploaded {local_path} to {target_path}")

#     def archive_cas(self, local_path: str, metadata: dict[str, Any] | None=None) -> str:
#         """
#         Archives a file using Content Addressable Storage (CAS).
#         Works on S3, Azure, GCS, or Local via self.fs.
#         """
#         # 1. Generate the hash (The Content Address)
#         file_hash = calculate_sha256(local_path)

#         # 2. Create a partitioned path: /base/af/1c/af1c...
#         # This keeps directories small and performant across all cloud providers
#         hash_prefix = file_hash[:2]
#         hash_subprefix = file_hash[2:4]

#         # self.url is the base archive path (e.g., 's3://my-archive')
#         remote_path = (
#             f"{self.url.rstrip('/')}/{hash_prefix}/{hash_subprefix}/{file_hash}"
#         )

#         # 3. Check for existence (Deduplication)
#         if not self.fs.exists(remote_path):
#             LOG.info(f"Storing new unique file: {file_hash}")
#             self.fs.put(local_path, remote_path)

#             # 4. Optional: Store metadata as a sidecar file if cloud-native tags aren't enough
#             if metadata:
#                 import json
#                 meta_path = f"{remote_path}.meta.json"
#                 with self.fs.open(meta_path, "w") as f:
#                     json.dump(metadata, f)
#         else:
#             LOG.info(f"Deduplication triggered: {file_hash} already exists.")

#         return file_hash


#     def fetch_df(self, raw_stream: io.BytesIO) -> tuple[BytesIO, Any | Literal['utf-8']]:
#         """
#         Peeks at the start of a binary stream to infer encoding
#         and returns a wrapper or a decoded buffer.
#         """
#         raw_stream = self.fs.open(self.url, mode="rb")

#         # 1. Read a sample (usually 16KB-32KB is plenty)
#         sample = raw_stream.read(32768)

#         # 2. Detect encoding
#         results = from_bytes(sample)
#         best_match = results.best()

#         # Fallback to utf-8 if detection is uncertain
#         encoding = best_match.encoding if best_match else "utf-8"
#         LOG.info(
#             f"🔍 Detected encoding: {encoding} (Confidence: {best_match.rating if best_match else 'N/A'})"
#         )

#         # 3. Handle the "Reset"
#         # Since we read the sample, we must 'rewind' the stream for the reader
#         try:
#             stream, detected_enc = raw_stream.seek(0)

#             # Now Polars knows exactly how to decode the bytes
#             return pl.read_csv(stream, encoding=detected_enc)
#         except (AttributeError, io.UnsupportedOperation):
#             # If the stream doesn't support seek (like some SFTP streams),
#             # we must prepend the sample back to the stream
#             return io.BytesIO(sample + raw_stream.read()), encoding


#     def quarantine(self, reason: str) -> None:
#         """
#         Moves the offending file to a 'quarantine' bucket/folder
#         and writes a JSON report.
#         """
#         quarantine_base = "s3://my-platform-data/quarantine"
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         filename = self.url.split("/")[-1]

#         dest_path = f"{quarantine_base}/{timestamp}_{filename}"
#         meta_path = f"{dest_path}.error.json"

#         # 1. Move the corrupted file
#         LOG.error(f"☣️ Quarantining {self.url} to {dest_path}")
#         self.fs.mv(self.url, dest_path)

#         # 2. Write the 'Death Certificate'
#         error_report = {
#             "source_url": self.url,
#             "error_reason": reason,
#             "timestamp": timestamp,
#             "file_size": self.fs.size(dest_path) if self.fs.exists(dest_path) else 0,
#         }

#         # Use fsspec to write the metadata directly to S3/SFTP
#         with self.fs.open(meta_path, "w") as f:
#             json.dump(error_report, f, indent=4)

#     def update_manifest(self, file_hash: str, original_meta: dict[str, Any]) -> None:
#         """
#         Maintains a 'manifest.jsonl' in the root of the archive.
#         JSONL (JSON Lines) is better for appending than standard JSON.
#         """
#         manifest_path = f"{self.url.rstrip('/')}/ingestion_manifest.jsonl"

#         entry = {
#             "hash": file_hash,
#             "timestamp": datetime.now().isoformat(),
#             "original_name": original_meta.get("filename"),
#             "size": original_meta.get("size"),
#             "source": original_meta.get("source_system"),
#         }

#         # Append to the manifest file (fsspec handles this across S3/Azure/Local)
#         with self.fs.open(manifest_path, "a") as f:
#             f.write(json.dumps(entry) + "\n")

#     def garbage_collect(self, dry_run: bool=True) -> None:
#         """
#         Compares the physical files in storage vs. the manifest.
#         Deletes files that aren't registered (orphaned files).
#         """
#         LOG.info("🧹 Starting Garbage Collection...")
#         # 1. Load all hashes from manifest
#         # 2. Crawl the filesystem for physical files
#         # 3. If file exists but hash is not in manifest -> delete
#         pass


# def rebuild_metadata_from_manifest(archive_url: str, db_connection: Connection) -> None:
#     fs = fsspec.filesystem(archive_url.split("://")[0])
#     manifest_path = f"{archive_url}/ingestion_manifest.jsonl"

#     with fs.open(manifest_path, "r") as f:
#         for line in f:
#             record = json.loads(line)
#             # Sync back to SQL
#             db_connection.execute(
#                 "INSERT INTO registry (hash, name) VALUES (?, ?)",
#                 (record["hash"], record["original_name"]),
#             )
#     print("✅ Metadata reconstructed successfully.")
