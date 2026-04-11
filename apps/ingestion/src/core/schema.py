import time

import polars as pl
import structlog
from apps.ingestion.src.core.strategies.extract import ReaderContext
from libs.database import TypeResolver

LOG = structlog.getLogger(__name__)


# TODO: Masking: Hash, Redact, Last_4
def apply_schema_contract(df: pl.DataFrame, context: ReaderContext) -> pl.DataFrame:
    """
    Decision: Stateless, Pure Function for Distributed Guarding.
    Enforces the physical contract.
    Handles: String-first casting, Masking, Renaming, and Column Selection.

    By applying this inside the Ray worker, we utilize the cluster CPU
    for hashing/casting and reduce memory usage by dropping extra cols immediately.
    """
    schema_items = context.schema_items
    if not schema_items:
        return df

    exprs = []
    for item in schema_items:
        s_col = item.get("source_col")
        t_col = item["target_col"]

        # 1. Resolve Polars type using the Canonical Mapper
        target_ptype = TypeResolver.resolve(context.source_type, item["target_dtype"])

        # 1. Handle Audit Columns (Internal Flag + No Source Column)
        if not s_col and t_col.startswith("_"):
            if t_col == "_ingested_at":
                expr = pl.lit(time.time())
                continue

            if t_col == "_partition":
                expr = pl.lit(context.partition_date)
                continue

            if t_col == "_run_id":
                expr = pl.lit(context.run_id)
                continue

            if t_col == "_source_host":
                expr = pl.lit(context.source_identifier)

            else:
                expr = pl.lit(None)

            exprs.append(expr.cast(pl.Utf8).alias(t_col))
            continue

        # 2. Defensive: String-first to avoid type-inference crashes
        expr = pl.col(s_col).cast(pl.Utf8)

        # 2. Masking (Default: Hash)
        mask_type = item.get("masking_type", "hash")
        if mask_type == "hash":
            # Fast Rust-based hashing
            expr = expr.str.hash(seed=42).cast(pl.Utf8)
        elif mask_type == "fixed":
            expr = pl.lit("MASKED_VALUE")

        # 3. Final Cast & Rename
        exprs.append(expr.cast(target_ptype).alias(t_col))

    # Single pass selection: Renames, Casts, and Drops extra columns
    return df.select(exprs)


# def generate_quarantine_report(lf: pl.LazyFrame):
#     # Only collect a tiny sample for debugging (e.g., 100 rows)
#     report_sample = lf.filter(pl.col("_is_quarantined")).limit(100).collect()

#     # This matches your 'masked_samples' requirement without loading 50M rows
#     return report_sample.to_dicts()
#     return report_sample.to_dicts()
