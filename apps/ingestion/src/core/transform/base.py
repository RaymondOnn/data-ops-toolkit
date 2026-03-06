import importlib
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

import structlog
import polars as pl

LOG = structlog.getLogger(__name__)


class TransformFactory:
    @staticmethod
    def get_strategy(job_id: str) -> "TransformStrategy":
        try:
            # Look for a module named 'src.custom.transforms.job_id'
            module = importlib.import_module(f"src.custom.transforms.{job_id}")
            # Expecting a class named 'CustomTransform' in that file
            return module.CustomTransform()
        except (ImportError, AttributeError):
            LOG.info(f"No custom transform for {job_id}, falling back to Default.")
            return DefaultTransformStrategy()


class TransformStrategy(ABC):
    """
    The 'Contract' for all transformation logic.
    """
    @abstractmethod
    def apply(self, lf: pl.LazyFrame, config: Any) -> pl.LazyFrame:
        """
        Add transformation steps to the lazy plan.
        Do NOT call .collect() here!
        """
        pass

    def _get_lazy_source(self, input_path: Path) -> pl.LazyFrame:
        """Helper: Standard way to load the Bronze data."""
        return pl.scan_parquet(input_path / "*.parquet")


class NoOpTransformStrategy(TransformStrategy):
    def apply(self, input_path: Path, output_path: Path) -> Dict[str, Any]:
        # Lazy scan all raw parquet
        lf = pl.scan_parquet(input_path / "*.parquet")
        
        # Write to transform folder as a single file (or same shard)
        output_file = output_path / "transformed_data.parquet"
        lf.sink_parquet(output_file)
        
        return {
            "rows": lf.collect().height,  # Total rows preserved
            "valid_schema": True,
            "schema": {k: str(v) for k, v in lf.schema.items()}
        }


class DefaultTransformStrategy(TransformStrategy):
    """
    The 'Sane Defaults' strategy. 
    Handles common data cleaning tasks based on JobConfig flags.
    """
    version = "1.0.0"

    def apply(self, lf: pl.LazyFrame, config: Any) -> pl.LazyFrame:
        # 1. STANDARDIZE: Column naming (snake_case)
        # Removes spaces, special characters, and forces lowercase
        lf = self._standardize_column_names(lf)

        # 2. DEDUPLICATE: If a primary key is provided in config
        if hasattr(config, 'primary_key') and config.primary_key:
            # We sort by a timestamp if available to keep the 'latest'
            sort_col = getattr(config, 'timestamp_col', None)
            if sort_col in lf.columns:
                lf = lf.sort(sort_col, descending=True)
            
            lf = lf.unique(subset=[config.primary_key], keep="first")

        # 3. NULL HANDLING: Drop rows missing critical business data
        if hasattr(config, 'required_columns') and config.required_columns:
            # Clean column names in config to match our standardized names
            req_cols = [c.lower().replace(" ", "_") for c in config.required_columns]
            lf = lf.drop_nulls(subset=req_cols)

        # 4. FLATTENING: Auto-unpacking Struct columns
        # Useful for API responses that nest data
        lf = self._auto_flatten_structs(lf)

        return lf

    def _standardize_column_names(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        mapping = {
            col: col.lower().strip().replace(" ", "_").replace("-", "_") 
            for col in lf.columns
        }
        return lf.rename(mapping)

    def _auto_flatten_structs(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Unpacks Polars Struct types into top-level columns."""
        struct_cols = [col for col, dtype in lf.schema.items() if dtype == pl.Struct]
        for col in struct_cols:
            lf = lf.unnest(col)
        return lf
        
        
