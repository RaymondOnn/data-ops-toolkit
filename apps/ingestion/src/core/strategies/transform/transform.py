from typing import Any, ClassVar

import polars as pl
from loguru import logger

from .base import TransformContext, Transformer
from .factory import TransformFactory

LOG = logger


@TransformFactory.register("skip")
class NoOpTransformer(Transformer):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def apply(self, lf: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        """Simply returns the LazyFrame as is."""
        return lf


@TransformFactory.register("default")
class DefaultTransformer(Transformer):
    """
    Standard normalization layer for all datasets.
    Ensures structural consistency and uniqueness before the load stage.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def apply(self, lf: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        # 1. STANDARDIZE: Column naming (snake_case)
        lf = self._standardize_column_names(lf)

        # 2. CLEAN: Trim whitespace from all string columns
        # We do this before empty-to-null conversion so that '  ' becomes null
        lf = lf.with_columns(pl.col(pl.Utf8).str.strip_chars())

        # 3. NULL CONVERSION: Convert empty strings ('') to NULL
        # Critical for DB integrity so that whitespace-only values don't
        # become blank entries
        # Using .replace preserves column names and is more concise
        lf = lf.with_columns(pl.col(pl.Utf8).replace("", None))

        # 4. DEDUPLICATE: Global uniqueness at the record level
        return lf.unique(keep="first")

        # # 5. AUTO-FLATTEN: Unpack nested Structs
        # lf = self._auto_flatten_structs(lf)

    def _standardize_column_names(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Forces snake_case and removes special characters."""
        # Use .name.map to rename columns without resolving the schema
        return lf.select(
            pl.all().name.map(
                lambda col: col.lower().strip().replace(" ", "_").replace("-", "_")
            )
        )

    def _empty_strings_to_nulls(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Converts empty or whitespace-only strings back to nulls.
        Ensures target databases receive clean NULL values.
        """
        return lf.with_columns(
            pl.col(pl.Utf8).map_elements(
                lambda s: None if s is not None and s.strip() == "" else s,
                return_dtype=pl.Utf8,
            )
        )

    def _auto_flatten_structs(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Unpacks Polars Struct types into top-level columns."""
        struct_cols = [
            col for col, dtype in lf.collect_schema().items() if dtype == pl.Struct
        ]
        if struct_cols:
            lf = lf.unnest(struct_cols)
        return lf


@TransformFactory.register("bitmask")
class BitmaskTransformer(Transformer):
    # Stage 1: The Decipher Map
    _OPS: ClassVar[dict[int, str]] = {
        1: "TRIM",
        2: "DROP_NULL",
        4: "CAST_STR",
        8: "LOWERCASE",
        16: "UPPERCASE",
        32: "TO_DATE",
        64: "COALESCE_0",
        128: "ROUND_2",
        256: "DIGITS_ONLY",
    }

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def apply(self, lf: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        # STAGE 1: Decipher and Reorganize into Per-Operation Buckets
        buckets: dict[str, set[str]] = {}
        schema_names = lf.collect_schema().names()
        for col_name, mask in ctx.options.get("column_masks", set()):
            if col_name not in schema_names:
                continue
            for bit, op_label in self._OPS.items():
                if int(mask) & bit:
                    buckets.setdefault(op_label, set()).add(col_name)

        if not buckets:
            return lf

        # STAGE 2: Define and Apply Horizontal Transformations
        # Mapping labels to Polars expressions (must be lazy-evaluated
        # with column lists)
        horizontal_transforms = {
            "CAST_STR": lambda cols: pl.col(cols).cast(pl.Utf8),
            "TRIM": lambda cols: pl.col(cols).str.strip_chars(),
            "LOWERCASE": lambda cols: pl.col(cols).str.to_lowercase(),
            "UPPERCASE": lambda cols: pl.col(cols).str.to_uppercase(),
            "DIGITS_ONLY": lambda cols: pl.col(cols).str.replace_all(r"\D", ""),
            "ROUND_2": lambda cols: pl.col(cols).round(2),
            "COALESCE_0": lambda cols: pl.col(cols).fill_null(0),
            "TO_DATE": lambda cols: pl.col(cols).str.to_date(),
        }

        exprs = [
            op_func(cols)
            for op_label, op_func in horizontal_transforms.items()
            if (cols := buckets.get(op_label))
        ]

        if exprs:
            lf = lf.with_columns(exprs)

        # STAGE 3: Execute vertical transforms (Filtering)
        if drop_cols := buckets.get("DROP_NULL"):
            lf = lf.drop_nulls(subset=drop_cols)

        return lf
