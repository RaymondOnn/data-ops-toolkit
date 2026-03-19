import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, cast

import structlog
import polars as pl

LOG = structlog.getLogger(__name__)


class Transformer(ABC):
    """
    The 'Contract' for all transformation logic.
    """

    @abstractmethod
    def apply(self, lf: pl.LazyFrame, **params: Any) -> None:
        """
        Add transformation steps to the lazy plan.
        Do NOT call .collect() here!
        """
        raise NotImplementedError("Subclasses must implement this method")


class NoOpTransformer(Transformer):
    def apply(self, lf: pl.LazyFrame, **params: Any) -> None:
        # Write to transform folder as a single file (or same shard)
        output_file = Path(output_folder) / "transformed_data.parquet"
        lf.sink_parquet(output_file)

        # return {
        #     "rows": lf.collect().height,  # Total rows preserved
        #     "valid_schema": True,
        #     "schema": {k: str(v) for k, v in lf.schema.items()}
        # }


class DefaultTransformer:
    """
    Standard normalization layer for all datasets.
    Ensures structural consistency and uniqueness before the load stage.
    """

    def apply(self, lf: pl.LazyFrame, **params: Any) -> None:
        # 1. STANDARDIZE: Column naming (snake_case)
        lf = self._standardize_column_names(lf)

        # 2. CLEAN: Trim whitespace from all string columns
        # We do this before empty-to-null conversion so that '  ' becomes null
        lf = lf.with_columns(pl.col(pl.Utf8).str.strip_chars())

        # 3. NULL CONVERSION: Convert empty strings ('') to NULL
        # Critical for DB integrity so that whitespace-only values don't become blank entries
        lf = lf.with_columns(
            pl.col(pl.Utf8).map_elements(lambda s: None if s == "" else s, return_dtype=pl.Utf8)
        )

        # 4. DEDUPLICATE: Global uniqueness via metadata hash
        if "_row_hash" in lf.columns:
            lf = lf.unique(subset=["_row_hash"], keep="first")

        # # 5. AUTO-FLATTEN: Unpack nested Structs
        # lf = self._auto_flatten_structs(lf)

    def _standardize_column_names(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Forces snake_case and removes special characters."""
        mapping = {
            col: col.lower().strip().replace(" ", "_").replace("-", "_") for col in lf.columns
        }
        return lf.rename(mapping)

    def _empty_strings_to_nulls(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Converts empty or whitespace-only strings back to nulls.
        Ensures target databases receive clean NULL values.
        """
        return lf.with_columns(
            pl.col(pl.Utf8).map_elements(
                lambda s: None if s is not None and s.strip() == "" else s, return_dtype=pl.Utf8
            )
        )

    def _auto_flatten_structs(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Unpacks Polars Struct types into top-level columns."""
        struct_cols = [col for col, dtype in lf.schema.items() if dtype == pl.Struct]
        if struct_cols:
            lf = lf.unnest(struct_cols)
        return lf


class BitmaskTransformer:
    # Stage 1: The Decipher Map
    _OPS = {
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

    def apply(self, lf: pl.LazyFrame, column_masks: list[tuple[str, int]]) -> pl.LazyFrame:
        # STAGE 1 & 2: Decipher and Reorganize into Per-Operation Buckets
        buckets: dict[str, list[str]] = {}
        for col_name, mask in column_masks:
            if col_name not in lf.columns:
                continue
            for bit, op_label in self._OPS.items():
                if int(mask) & bit:
                    buckets.setdefault(op_label, []).append(col_name)

        if not buckets:
            return lf

        # STAGE 3: Apply Transformations (Batch grouped)
        exprs = []

        # String Operations
        if "CAST_STR" in buckets:
            exprs.append(pl.col(buckets["CAST_STR"]).cast(pl.Utf8))
        if "TRIM" in buckets:
            exprs.append(pl.col(buckets["TRIM"]).str.strip_chars())
        if "LOWERCASE" in buckets:
            exprs.append(pl.col(buckets["LOWERCASE"]).str.to_lowercase())
        if "UPPERCASE" in buckets:
            exprs.append(pl.col(buckets["UPPERCASE"]).str.to_uppercase())
        if "DIGITS_ONLY" in buckets:
            exprs.append(pl.col(buckets["DIGITS_ONLY"]).str.replace_all(r"\D", ""))

        # Numeric/Date Operations
        if "ROUND_2" in buckets:
            exprs.append(pl.col(buckets["ROUND_2"]).round(2))
        if "COALESCE_0" in buckets:
            exprs.append(pl.col(buckets["COALESCE_0"]).fill_null(0))
        if "TO_DATE" in buckets:
            exprs.append(pl.col(buckets["TO_DATE"]).str.to_date())

        # Execute all horizontal transforms in ONE pass
        if exprs:
            lf = lf.with_columns(exprs)

        # Execute vertical transforms (Filtering)
        if "DROP_NULL" in buckets:
            lf = lf.drop_nulls(subset=buckets["DROP_NULL"])

        return lf


class TransformFactory:
    """
    Decision: Dynamic Module Loading.
    Allows for job-specific logic (e.g., complex bitmasking for a specific vendor)
    without bloating the core engine codebase.
    """

    @staticmethod
    def get_transformer(job_id: str) -> "Transformer":
        import importlib

        # Example: job_sales_daily -> JobSalesDaily
        class_name = "".join(x.capitalize() for x in re.split(r"[-_]", job_id))
        module_path = f"src.core.transform.custom.{job_id}"

        try:
            module = importlib.import_module(module_path)
            # Dynamically get the class named after the job_id
            transformer_class = getattr(module, class_name)
            return cast(Transformer, transformer_class)()
        except (ImportError, AttributeError):
            LOG.info("Fallback to DefaultTransformer", job_id=job_id)
            return DefaultTransformer()
