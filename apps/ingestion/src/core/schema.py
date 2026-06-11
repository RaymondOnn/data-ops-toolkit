import time
from typing import Any

import msgspec
import polars as pl
from libs.database import TypeResolver


# TODO: Masking: Hash, Redact, Last_4
class ColumnMapping(msgspec.Struct):
    """Represents a single column mapping and transformation rule.

    Attributes:
        target_col: The canonical name of the column in the destination.
        target_type: The target data type identifier (e.g., 'int64', 'string').
        source_col: The original name in the source system.
        masking: The PII protection strategy ('hash', 'fixed', 'none').
    """

    target_col: str
    target_type: str
    source_col: str | None = None
    masking: str = "none"

    def __post_init__(self):
        """Normalize source_col representation."""
        if self.source_col in ("None", "null", ""):
            self.source_col = None


def apply_schema_contract(df: pl.DataFrame, context: Any) -> pl.DataFrame:
    """Enforces the physical schema contract on an extracted Polars DataFrame."""
    if not context.schema or df.width == 0:
        return df
    exprs = [_build_column_expr(item, context) for item in context.schema]
    return df.select(exprs)


def _build_column_expr(item: ColumnMapping, context: Any) -> pl.Expr:
    """Build a single column transformation expression."""
    # Audit/literal columns (system metadata)
    if item.source_col is None and item.target_col.startswith("_"):
        expr = _get_audit_literal(item.target_col, context)
    else:
        # Standard column with optional masking
        expr = pl.col(str(item.source_col)).cast(pl.String)
        # expr = _apply_masking(expr, item.masking)

    # Apply final type and alias
    target_polars_type = TypeResolver.resolve_to_polars(context.kind, item.target_type)
    return expr.cast(target_polars_type).alias(item.target_col)


def _get_audit_literal(col_name: str, context: Any) -> pl.Expr:
    """Returns the appropriate audit literal expression for system columns."""
    audit_map = {
        "_created_at_ts": lambda: pl.lit(time.time()),
        "_partition": lambda: pl.lit(str(getattr(context, "partition_date", None))),
        "_run_id": lambda: pl.lit(context.run_id),
        "_source": lambda: pl.lit(getattr(context, "resource", None)),
    }
    return audit_map.get(col_name, lambda: pl.lit(None))()


# TODO
def _apply_masking(expr: pl.Expr, strategy: str) -> pl.Expr:
    """Apply PII masking strategy to an expression."""
    strategy = strategy.lower()
    if strategy == "hash":
        return expr.hash(seed=42).cast(pl.String)
    if strategy == "fixed":
        return pl.lit("MASKED_VALUE")
    return expr  # 'none' or unknown strategy
