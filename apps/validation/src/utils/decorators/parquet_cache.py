import functools
from pathlib import Path

import polars as pl

def parquet_cache(cache_dir="./audit_cache"):
    """
    Decorator to 'Tee' a LazyFrame stream into a local Parquet file
    during the first execution.
    """
    Path(cache_dir).mkdir(parents=True, exist_ok=True)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Generate a stable filename based on the dataset name
            cache_file = Path(cache_dir) / f"{self.name}.parquet"
            
            # If cache exists, bypass the source entirely
            if cache_file.exists():
                return pl.scan_parquet(str(cache_file))

            # If not, get the LazyFrame from the original method
            lf = func(self, *args, **kwargs)
            
            # Use 'sink_parquet' to execute the stream and save to disk
            # while keeping memory usage low. 
            # Note: We return the scan of the newly created file.
            lf.sink_parquet(str(cache_file))
            return pl.scan_parquet(str(cache_file))
        return wrapper
    return decorator

# Usage

# @parquet_cache()
# def get_raw_stream(self):
#     return pl.scan_ipc(self.path)