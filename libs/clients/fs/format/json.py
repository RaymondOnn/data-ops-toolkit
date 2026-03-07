import re
import io
import polars as pl
from .base import FormatHandler

class JSONHandler(FormatHandler):
    def read_mem(self, target: str, **kwargs) -> io.BytesIO:
        """Handles 'Trailing Comma' repairs for JSON."""
        encoding = kwargs.get("encoding", "utf-8")
        with self.fs.open(target, "rb") as f:
            content = f.read().decode(encoding, errors="ignore")
            
            # Repair: [1, 2, 3,] -> [1, 2, 3]
            # This regex looks for commas followed by a closing brace/bracket
            clean_content = re.sub(r",\s*([\]}])", r"\1", content)
            
            return io.BytesIO(clean_content.encode("utf-8"))

    def to_df(self, target: str, **kwargs) -> pl.LazyFrame:
        size = self.fs.size(target)
        if size > 1.5 * 1024**3: # 1.5GB warning
            self.logger.warning(f"Standard JSON {target} is very large. Risk of OOM.")
        
        # Performance: For Newline Delimited JSON (ndjson), 
        # Polars can scan it lazily without loading into memory.
        if target.lower().endswith(".ndjson") or target.lower().endswith(".jsonl"):
            return pl.scan_ndjson(target, storage_options=self.opts)

        # Standard JSON requires full load into memory for repair
        buffer = self.read_mem(target, **kwargs)
        return pl.read_json(buffer).lazy()

    def from_df(self, lf: pl.LazyFrame, target: str):
        """Writes as NDJSON for better 50M row performance."""
        # Polars sink_ndjson is much faster than standard sink_json
        lf.sink_ndjson(target)

    def write_file(self, data: bytes, target: str):
        with self.fs.open(target, "wb") as f:
            f.write(data)
            
class JSONLHandler(FormatHandler):
    def read_mem(self, target: str, **kwargs) -> io.BytesIO:
        """Reads raw JSONL into memory. (Caution: Eager)"""
        with self.fs.open(target, "rb") as f:
            return io.BytesIO(f.read())

    def to_df(self, target: str, **kwargs) -> pl.LazyFrame:
        """
        High-Performance: Uses scan_ndjson for 50M row safety.
        Supports 'ignore_errors' to skip malformed lines (Self-healing).
        """
        return pl.scan_ndjson(
            target, 
            storage_options=self.opts,
            ignore_errors=kwargs.get("ignore_errors", True)
        )

    def from_df(self, lf: pl.LazyFrame, target: str):
        """Streaming write: Does not load data into RAM."""
        lf.sink_ndjson(target)

    def write_file(self, data: bytes, target: str):
        with self.fs.open(target, "wb") as f:
            f.write(data)