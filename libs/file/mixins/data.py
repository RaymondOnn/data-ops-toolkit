import logging
import io
from typing import Optional, Union, Callable, Any

import polars as pl
import fsspec

from libs.file.formats.base import HandlerFactory

LOG = logging.getLogger(__name__)

class FlatFileMixin:
    
    fs: fsspec.AbstractFileSystem
    opts: dict[str, Any]
    resolve_path: Callable[[str], str]
    
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

    def fetch_df(
        self, 
        path_or_list: Union[str, list[str]], 
        file_pattern: Optional[str] = None, 
        force_repair: bool = False,
        **kwargs: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Orchestrates the ingestion using specialized FormatHandlers.
        """
        # 1. Resolve targets (Folder, Archive, or Pre-partitioned list)
        if isinstance(path_or_list, list):
            fs, targets = self.fs, path_or_list
        else:
            fs, targets = self.get_reader_context(path_or_list, file_pattern)

        lfs = []
        for target in targets:
            # 2. Step 2.5: Sanity Check (Exists? Zero-byte?)
            if not self.validate_before_read(fs, target):
                continue

            # 3. Identify Handler & Detect Encoding
            ext = target.split('.')[-1].lower()
            handler = HandlerFactory.get_handler(ext, fs, getattr(self, 'opts'))

            # Step 3: Encoding Detection (Crucial for CSV/JSON/XML)
            # Only run if not Parquet to save cycles
            encoding = "utf-8"
            if ext != "parquet":
                _, encoding = self._get_encoded_stream(fs, target)

            # 4. Delegate to Handler (Handles Streaming vs Repair internally)
            # Note: 50M row safety happens inside handler.to_df()
            lf = handler.to_df(
                target, 
                encoding=encoding, 
                force_repair=force_repair,
                **kwargs
            )
            lfs.append(lf)

        # 5. Final Consolidation
        # concat is a lazy operation in Polars; no memory is consumed yet.
        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def write_data(self, lf: pl.LazyFrame, path: str) -> None:
        """
        Unified write entry point. Uses optimized streaming sinks 
        (sink_parquet, sink_ndjson, sink_csv) via handlers.
        """
        full_path = self.resolve_path(path)
        ext = full_path.split('.')[-1].lower()
        handler = HandlerFactory.get_handler(ext, self.fs, getattr(self, 'opts'))

        # Ensure directory exists for local paths
        self.fs.makedirs(self.fs._parent(full_path), exist_ok=True)

        LOG.info(f"Starting high-volume write to: {full_path}")
        handler.from_df(lf, full_path)

    def get_reader_context(self, path: str, pattern: Optional[str] = None) -> tuple[fsspec.AbstractFileSystem, list[str]]:
        """
        Internal helper to resolve targets (Folder, Archive, or Pre-partitioned list)
        """
        import fnmatch

        full_path = self.resolve_path(path)
        archive_map = {".zip": "zip", ".tar": "tar", ".tar.gz": "tar", ".tgz": "tar", ".gz": "gzip"}
        ext = next((e for e in archive_map if full_path.lower().endswith(e)), None)

        if ext:
            fs = fsspec.filesystem(archive_map[ext], fo=full_path, remote_options=getattr(self, 'opts'))
            all_files = fs.find("")
        else:
            fs = self.fs
            all_files = fs.find(full_path) if fs.isdir(full_path) else [full_path]

        # Selection Logic
        if pattern:
            matches = fnmatch.filter(all_files, f"*{pattern}*")
            if not matches:
                raise FileNotFoundError(f"Pattern {pattern} not found in {full_path}")
            targets = matches
        else:
            data_exts = ('.csv', '.parquet', '.json')
            targets = [f for f in all_files if f.lower().endswith(data_exts)]

        if not targets:
            raise FileNotFoundError(f"No valid data files identified in {full_path}")

        return fs, targets

    def _get_encoded_stream(self, fs: fsspec.AbstractFileSystem, target: str) -> tuple[io.IOBase, str]:
        """
        Internal helper to detect encoding and provide a 'rewindable' or 
        reconstructed stream for Polars.
        """
        from charset_normalizer import from_bytes

        raw_stream = fs.open(target, mode="rb")

        # 1. Read a sample for detection
        sample = raw_stream.read(32768)
        results = from_bytes(sample)
        best_match = results.best()
        encoding = best_match.encoding if best_match else "utf-8"

        LOG.info(
            "Encoding detected", 
            extra={
                "file": target, 
                "encoding": encoding, 
                "confidence": best_match.rating if best_match else "N/A"
            }
        )

        # 2. Handle the "Reset" / Rewind logic
        try:
            # Attempt to seek back to the start if the filesystem supports it
            raw_stream.seek(0)
            return raw_stream, encoding
        except (AttributeError, io.UnsupportedOperation):
            # Fallback for streams that don't support seek (e.g., some SFTP/S3 wrappers)
            # We prepend the sample back to the remaining data
            LOG.debug("Stream not seekable; reconstructing via BytesIO")
            full_content = sample + raw_stream.read()
            return io.BytesIO(full_content), encoding

    def get_load_strategy(self, path: str, file_pattern: str | None = None) -> list[list[str]]:
        """
        Splits a folder/archive into 1GB chunks so each Ray worker/batch 
        stays safely under the 2GB limit.
        """
        fs, targets = self.get_reader_context(path, file_pattern)

        partitions = []
        current_batch: list[str] = []
        current_size = 0
        LIMIT = 1 * 1024**3 # 1GB target per partition

        for t in targets:
            size = fs.size(t)
            if current_size + size > LIMIT and current_batch:
                partitions.append(current_batch)
                current_batch = []
                current_size = 0

            current_batch.append(t)
            current_size += size

        if current_batch:
            partitions.append(current_batch)

        return partitions
