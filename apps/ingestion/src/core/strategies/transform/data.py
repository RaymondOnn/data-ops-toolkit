"""Ray-based distributed transformation executor."""

# from __future__ import annotations

from typing import Any, cast

import polars as pl
import ray
from loguru import logger

from .base import TransformContext, Transformer
from .factory import TransformFactory

LOG = logger


class DataTransformer(Transformer):
    """Execute transformations using Ray for distribution."""

    def transform(self, context: TransformContext) -> tuple[int, dict[str, str]]:
        LOG.info(f"Loading data from {context.source}")
        ds = ray.data.read_parquet(str(context.source))

        def process_batch(batch: Any) -> Any:
            # Step 1: Convert Arrow batch to Polars DataFrame
            df = pl.from_arrow(batch)

            # Handle Series (single column) case
            if isinstance(df, pl.Series):
                df = df.to_frame()

            # Convert to LazyFrame for transformation
            lf = df.lazy()

            # Get and apply transformation logic
            logic = TransformFactory.get(
                context.logic, job_id=context.job_id, dataset_id=context.dataset_id
            )
            transformed = logic.apply(lf, context)

            # Ensure result is LazyFrame (it should be)
            if not isinstance(transformed, pl.LazyFrame):
                raise TypeError(f"Expected LazyFrame, got {type(transformed)}")

            # Collect to DataFrame for Arrow conversion
            collected = transformed.collect()

            # Return as Arrow
            return cast("pl.DataFrame", collected).to_arrow()

        ds.map_batches(process_batch, batch_format="pyarrow").write_parquet(
            str(context.target)
        )

        # return collected

        # # Use pandas batch format
        # ds.map_batches(process_batch, batch_format="pandas").write_parquet(
        #     str(context.target)
        # )

        # Get row count safely
        lf = pl.scan_parquet(str(context.target / "*.parquet"))
        row_df = lf.select(pl.len()).collect()

        row_count = (
            int(row_df[0, 0])
            if isinstance(row_df, pl.DataFrame) and not row_df.is_empty()
            else 0
        )

        # Get schema
        sample = next(context.target.glob("*.parquet"), None)
        schema = {}
        if sample:
            schema = {k: str(v) for k, v in pl.read_parquet_schema(sample).items()}

        LOG.info(f"Transformation complete: {row_count:_} rows")
        return row_count, schema
