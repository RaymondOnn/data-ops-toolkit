
import os
import polars as pl


# 1. Limit CPU to prevent thrashing
# In K8S, match this to your 'cpu' request/limit
os.environ["POLARS_MAX_THREADS"] = "2" 

# 2. Tell Polars where the 'Safe' spill zone is
os.environ["POLARS_TEMP_DIR"] = "/tmp/polars-spill"

# 3. Memory Throttling
def guarded_collect(lf: pl.LazyFrame):
    return lf.collect(
        streaming=True,
        # Smaller chunks = lower peak memory but slower processing
        streaming_chunk_size=50_000 
    )