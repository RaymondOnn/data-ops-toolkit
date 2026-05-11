import time

import polars as pl
from apps.ingestion.src.core.strategies.extract import ReaderContext
from libs.database import TypeResolver


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

    # Guard: If no schema is defined or the batch arrived without columns,
    # skip processing to avoid ColumnNotFound errors.
    if not schema_items or df.width == 0:
        return df

    exprs = []
    for item in schema_items:
        # 1. Normalize Source Column (Handle CSV 'None' strings)
        s_col = item.get("source_col")
        if s_col in (None, "None", "null", ""):
            s_col = None

        t_col = item["target_col"]

        # 2. Resolve Polars type using the Canonical Mapper
        target_ptype = TypeResolver.resolve_to_polars(
            context.source_type, item["target_dtype"]
        )

        # 3. Handle Audit/Literal Columns (No physical source column)
        if s_col is None and t_col.startswith("_"):
            if t_col in ("_created_at_ts", "_ingested_at"):
                expr = pl.lit(time.time())
            elif t_col == "_partition":
                expr = pl.lit(context.partition_date)
            elif t_col == "_run_id":
                expr = pl.lit(context.run_id)
            elif t_col in ("_source", "_source_host"):
                expr = pl.lit(context.source_identifier)
            else:
                expr = pl.lit(None)

            # Directly cast literal to target type and alias
            exprs.append(expr.cast(target_ptype).alias(t_col))

        else:
            # 4. Standard Columns: Cast to String first for stability
            expr = pl.col(str(s_col)).cast(pl.String)

            # 5. Masking Logic
            # Default to 'none' to prevent accidental hashing of non-PII columns like dates.
            mask_val = str(item.get("masking", "none")).lower()
            if mask_val == "hash":
                expr = expr.hash(seed=42).cast(pl.String)
            elif mask_val == "fixed":
                expr = pl.lit("MASKED_VALUE")

            # 6. Final Cast to target type (Int64, Date, etc.) and Rename
            exprs.append(expr.cast(target_ptype).alias(t_col))

    # Single pass selection: Renames, Casts, and Drops extra columns
    return df.select(exprs)
