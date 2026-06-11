"""Built-in transformer implementations."""

from typing import Any, ClassVar

import polars as pl
from apps.ingestion.src.core.strategies.transform.base import (
    TransformContext,
    TransformLogic,
)
from apps.ingestion.src.core.strategies.transform.factory import TransformFactory
from loguru import logger

LOG = logger


@TransformFactory.register("skip")
class PassthroughLogic(TransformLogic):
    """Pass data through unchanged."""

    def apply(self, df: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        return df


@TransformFactory.register("default")
class StandardLogic(TransformLogic):
    """Standard normalization for all datasets."""

    def apply(self, df: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        # 1. Standardize column names (snake_case)
        df = df.select(
            pl.all().name.map(
                lambda c: c.lower().strip().replace(" ", "_").replace("-", "_")
            )
        )

        # 2. Trim whitespace from strings
        df = df.with_columns(pl.col(pl.Utf8).str.strip_chars())

        # 3. Convert empty strings to NULL
        df = df.with_columns(pl.col(pl.Utf8).replace("", None))

        # 4. Remove duplicates
        return df.unique(keep="first")


@TransformFactory.register("bitmask")
class BitmaskLogic(TransformLogic):
    """Apply bitmask-based column transformations with batched expressions."""

    # Operation definitions
    _COLUMN_OPS: ClassVar[dict[int, tuple[str, Any]]] = {
        1: ("trim", lambda cols: pl.col(cols).str.strip_chars()),
        2: ("drop_null", None),  # Filter op
        4: ("cast_string", lambda cols: pl.col(cols).cast(pl.Utf8)),
        8: ("lowercase", lambda cols: pl.col(cols).str.to_lowercase()),
        16: ("uppercase", lambda cols: pl.col(cols).str.to_uppercase()),
        32: ("to_date", lambda cols: pl.col(cols).str.to_date()),
        64: ("fill_zero", lambda cols: pl.col(cols).fill_null(0)),
        128: ("round_2", lambda cols: pl.col(cols).round(2)),
        256: ("digits_only", lambda cols: pl.col(cols).str.replace_all(r"\D", "")),
    }

    def apply(self, df: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        masks = ctx.params.get("column_masks", set())
        if not masks:
            return df

        # Group columns by operation
        column_ops = {}  # name -> set of columns
        filter_ops = set()  # columns to drop nulls from
        schema = set(df.collect_schema().names())

        for col, mask in masks:
            if col not in schema:
                continue
            for bit, (name, _) in self._COLUMN_OPS.items():
                if int(mask) & bit:
                    if name == "drop_null":
                        filter_ops.add(col)
                    else:
                        column_ops.setdefault(name, set()).add(col)

        # Build ALL expressions at once
        expressions = []
        for name, cols in column_ops.items():
            for _, (op_name, builder) in self._COLUMN_OPS.items():
                if op_name == name and builder:
                    expressions.append(builder(cols))
                    break

        # Apply ALL column transformations in ONE pass (efficient for large data)
        if expressions:
            df = df.with_columns(expressions)

        # Apply filters after column transformations
        if filter_ops:
            df = df.drop_nulls(subset=filter_ops)

        return df
