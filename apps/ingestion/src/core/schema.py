


import polars as pl
import structlog

from libs.database import TARGET_TO_POLARS

LOG = structlog.getLogger(__name__)


#TODO: Masking: Hash, Redact, Last_4
def apply_schema_contract(df: pl.DataFrame, schema_items: list[dict[str, Any]]) -> pl.DataFrame:
    """
    Decision: Stateless, Pure Function for Distributed Guarding.
    Enforces the physical contract.
    Handles: String-first casting, Masking, Renaming, and Column Selection.
    
    By applying this inside the Ray worker, we utilize the cluster CPU 
    for hashing/casting and reduce memory usage by dropping extra cols immediately.
    """
    if not schema_items:
        return df

    exprs = []
    for item in schema_items:
        s_col = item['source_col']
        t_col = item['target_col']
        target_ptype = TARGET_TO_POLARS.get(item['target_dtype'], pl.Utf8)

        # 1. Defensive: String-first to avoid type-inference crashes
        expr = pl.col(s_col).cast(pl.Utf8)

        # 2. Masking (Default: Hash)
        mask_type = item.get('masking_type', 'hash')
        if mask_type == 'hash':
            # Fast Rust-based hashing
            expr = expr.str.hash(seed=42).cast(pl.Utf8)
        elif mask_type == 'fixed':
            expr = pl.lit("MASKED_VALUE")

        # 3. Final Cast & Rename
        exprs.append(expr.cast(target_ptype).alias(t_col))

    # Single pass selection: Renames, Casts, and Drops extra columns
    return df.select(exprs)

def generate_quarantine_report(lf: pl.LazyFrame):
    # Only collect a tiny sample for debugging (e.g., 100 rows)
    report_sample = (
        lf.filter(pl.col("_is_quarantined") == True)
        .limit(100)
        .collect()
    )
    
    # This matches your 'masked_samples' requirement without loading 50M rows
    return report_sample.to_dicts()