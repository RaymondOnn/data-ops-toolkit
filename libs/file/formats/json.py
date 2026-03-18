import re
import io
import logging
import polars as pl
from libs.file.formats.base import FormatHandler

LOG = logging.getLogger(__name__)

class JSONHandler(FormatHandler):
    def read_mem(self, target: str, **kwargs) -> io.BytesIO:
        """Handles 'Trailing Comma' repairs for standard JSON."""
        encoding = kwargs.get("encoding", "utf-8")
        with self.fs.open(target, "rb") as f:
            content = f.read().decode(encoding, errors="ignore")
            
            # Repair: [1, 2, 3,] -> [1, 2, 3]
            # This regex looks for commas followed by a closing brace/bracket
            clean_content = re.sub(r",\s*([\]}])", r"\1", content)
            
            return io.BytesIO(clean_content.encode("utf-8"))

    def to_df(self, target: str, **kwargs) -> pl.LazyFrame:
        """
        Unified Reader:
        1. High-Performance: Uses scan_ndjson for .ndjson/.jsonl or NDJSON-formatted .json.
        2. Defensive: Uses read_json + repair for standard .json (arrays).
        """
        is_ndjson = any(target.lower().endswith(ext) for ext in [".ndjson", ".jsonl"])
        
        if not is_ndjson:
            # Peek to see if it's NDJSON (starts with '{') or Standard JSON (starts with '[')
            try:
                with self.fs.open(target, "rb") as f:
                    # Read a small chunk to find the first non-whitespace character
                    chunk = f.read(1024).strip()
                    if chunk and chunk[0:1] == b"{":
                        is_ndjson = True
            except Exception:
                LOG.debug(f"Peeking failed for {target}, defaulting to standard JSON path.")

        if is_ndjson:
            return pl.scan_ndjson(
                target, 
                storage_options=self.opts,
                ignore_errors=kwargs.get("ignore_errors", True)
            )

        # Standard JSON requires full load into memory for repair
        size = self.fs.size(target)
        if size > 1.5 * 1024**3: # 1.5GB warning
            LOG.warning(f"Standard JSON {target} is very large. Risk of OOM.")
            
        buffer = self.read_mem(target, **kwargs)
        return pl.read_json(buffer).lazy()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, target: str) -> None:
        """
        Streaming write: ALWAYS uses NDJSON for better 50M row performance 
        and memory safety (2GB RAM limit).
        """
        if isinstance(df, pl.LazyFrame):
            df.sink_ndjson(target)
        else:
            df.write_ndjson(target)

    def write_file(self, data: bytes, target: str):
        with self.fs.open(target, "wb") as f:
            f.write(data)