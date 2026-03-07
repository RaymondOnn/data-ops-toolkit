import fsspec
import polars as pl

class S3Client:
    def __init__(self, bucket: str, **opts):
        self.bucket = bucket
        # Use fsspec to handle S3/Local/Azure abstractly
        self.fs = fsspec.filesystem("s3", **opts)

    def write_parquet(self, lf: pl.LazyFrame, path: str):
        """Streams LazyFrame to S3 to respect 2GB RAM limit."""
        # Polars sink_parquet is the most memory-efficient way to push 50M rows
        lf.sink_parquet(path)

    def delete_prefix(self, prefix: str):
        if self.fs.exists(prefix):
            self.fs.rm(prefix, recursive=True)

    def move_objects(self, source: str, target: str):
        """Moves files from staging to production."""
        self.delete_prefix(target) # Idempotency: Clear target before move
        self.fs.move(source, target, recursive=True)