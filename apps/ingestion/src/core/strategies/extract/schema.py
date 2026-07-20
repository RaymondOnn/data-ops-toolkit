import polars as pl
from apps.ingestion.src.core.strategies.extract import ExtractContext
from libs.database import TypeResolver
from loguru import logger

LOG = logger


def apply_schema_contract(df: pl.DataFrame, context: ExtractContext) -> pl.DataFrame:
    """Enforces the physical schema contract on an extracted Polars DataFrame."""

    if df.width == 0:
        return df

    # Retrieve configuration overrides (checking both context attributes and params)
    null_if_values = context.null_if

    # 1. Handle Null-Replacement (null_if)
    # Replaces garbage string values like ["NA", "n/a", "null"] with true Polars nulls (None)
    if null_if_values:
        # Build an expression that checks if a column's value is in null_if_values
        # We apply this strictly to string/categorical columns to prevent type mismatch errors
        string_cols = [
            col
            for col, dtype in zip(df.columns, df.dtypes, strict=False)
            if dtype in (pl.String, pl.Categorical)
        ]
        if string_cols:
            df = df.with_columns(
                [
                    pl.when(pl.col(col).is_in(null_if_values))
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                    for col in string_cols
                ]
            )

    # 2. Handle Schema / Columns Selection
    columns_override = context.columns
    if columns_override is None or len(columns_override) == 0:
        return df

    # Case A: An explicit 'columns' override list is provided
    if isinstance(columns_override, dict):
        select_exprs = []

        # Loop over columns in their EXACT native order as discovered in the file
        for col_name in df.columns:
            if col_name in columns_override:
                raw_target_type = columns_override[col_name]
                polars_type = TypeResolver.resolve_to_polars(
                    context.kind, raw_target_type
                )

                # Performance Optimization: Skip casting if the data types already match
                if df.schema[col_name] == polars_type:
                    select_exprs.append(pl.col(col_name))
                else:
                    select_exprs.append(pl.col(col_name).cast(polars_type))
            else:
                # Schema Evolution: Let unmapped fields flow through untouched in their original order
                select_exprs.append(pl.col(col_name))

    return df.select(select_exprs)


# def _build_column_expr(item: ColumnMapping, context: Any) -> pl.Expr:
#     """Build a single column transformation expression."""
#     # Audit/literal columns (system metadata)
#     if item.source_col is None and item.target_col.startswith("_"):
#         expr = _get_audit_literal(item.target_col, context)
#     else:
#         # Standard column with optional masking
#         expr = pl.col(str(item.source_col)).cast(pl.String)
#         # expr = _apply_masking(expr, item.masking)

#     # Apply final type and alias
#     target_polars_type = TypeResolver.resolve_to_polars(context.kind, item.target_type)
#     return expr.cast(target_polars_type).alias(item.target_col)


# def _get_audit_literal(col_name: str, context: Any) -> pl.Expr:
#     """Returns the appropriate audit literal expression for system columns."""
#     audit_map = {
#         "_created_at_ts": lambda: pl.lit(time.time()),
#         "_partition": lambda: pl.lit(str(getattr(context, "partition_date", None))),
#         "_run_id": lambda: pl.lit(context.run_id),
#         "_source": lambda: pl.lit(getattr(context, "resource", None)),
#     }
#     return audit_map.get(col_name, lambda: pl.lit(None))()
