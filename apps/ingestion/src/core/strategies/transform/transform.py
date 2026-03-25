from typing import ClassVar

import polars as pl
import structlog

from libs.file.formats import FormatFactory

from .base import TransformContext, Transformer
from .factory import TransformFactory

LOG = structlog.getLogger(__name__)


@TransformFactory.register("skip")
class NoOpTransformer(Transformer):
    def apply(self, lf: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        ctx.destination_dir.mkdir(parents=True, exist_ok=True)

        # Check if format matches source format
        src_fmt: set[str] = set()
        for file in ctx.source_dir.iterdir():
            src_fmt.add(file.suffix)

        if len(src_fmt) > 1:
            raise ValueError("Source directory contains multiple formats")

        if src_fmt != set(ctx.output_format):
            writer = FormatFactory.get_handler(ctx.output_format)
            for file in ctx.source_dir.iterdir():
                filename = file.stem
                output_file = ctx.destination_dir / f"{filename}.{ctx.output_format}"
                writer.from_df(lf, output_file)
        else:
            raise ValueError(f"Unsupported format: {ctx.output_format}")

        # return {
        #     "rows": lf.collect().height,  # Total rows preserved
        #     "valid_schema": True,
        #     "schema": {k: str(v) for k, v in lf.schema.items()}
        # }
        return lf


@TransformFactory.register("default")
class DefaultTransformer(Transformer):
    """
    Standard normalization layer for all datasets.
    Ensures structural consistency and uniqueness before the load stage.
    """

    def apply(self, lf: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        # 1. STANDARDIZE: Column naming (snake_case)
        lf = self._standardize_column_names(lf)

        # 2. CLEAN: Trim whitespace from all string columns
        # We do this before empty-to-null conversion so that '  ' becomes null
        lf = lf.with_columns(pl.col(pl.Utf8).str.strip_chars())

        # 3. NULL CONVERSION: Convert empty strings ('') to NULL
        # Critical for DB integrity so that whitespace-only values don't
        # become blank entries
        lf = lf.with_columns(
            pl.col(pl.Utf8).map_elements(
                lambda s: None if s == "" else s, return_dtype=pl.Utf8
            )
        )

        # 4. DEDUPLICATE: Global uniqueness via metadata hash
        if "_row_hash" in lf.columns:
            lf = lf.unique(subset=["_row_hash"], keep="first")

        # # 5. AUTO-FLATTEN: Unpack nested Structs
        # lf = self._auto_flatten_structs(lf)
        return lf

    def _standardize_column_names(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Forces snake_case and removes special characters."""
        mapping = {
            col: col.lower().strip().replace(" ", "_").replace("-", "_")
            for col in lf.columns
        }
        return lf.rename(mapping)

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
        struct_cols = [col for col, dtype in lf.schema.items() if dtype == pl.Struct]
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

    def apply(self, lf: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        # STAGE 1: Decipher and Reorganize into Per-Operation Buckets
        buckets: dict[str, list[str]] = {}
        for col_name, mask in ctx.options.get("column_masks", []):
            if col_name not in lf.columns:
                continue
            for bit, op_label in self._OPS.items():
                if int(mask) & bit:
                    buckets.setdefault(op_label, []).append(col_name)

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
