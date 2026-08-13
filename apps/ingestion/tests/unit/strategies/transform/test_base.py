from typing import cast
from unittest.mock import MagicMock

import polars as pl

from src.core.strategies.transform.base import (
    TransformContext,
    Transformer,
)


class MockTransformer(Transformer):
    """Simple concrete implementation to test the base class contract."""

    @property
    def version(self) -> str:
        """Returns the version of the transformer logic."""
        return "1.0.0"

    def apply(self, lf: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        # Simulates a transformation that adds a 'processed' flag
        return lf.with_columns(pl.lit(True).alias("_is_processed"))


def test_transformer_apply_lazy_execution():
    """
    GIVEN a Polars LazyFrame
    WHEN the transformer's apply method is called
    THEN it should return a new LazyFrame with the transformation plan applied
    """
    transformer = MockTransformer(dataset_id="test", job_id="test")
    ctx = MagicMock()

    # Create dummy data
    df = pl.DataFrame({"id": [1, 2, 3]})
    result_lf = transformer.apply(df.lazy(), ctx)

    # Ensure it is still a LazyFrame (not collected yet)
    assert isinstance(result_lf, pl.LazyFrame)

    # Verify the results after collection
    result_df = cast("pl.DataFrame", result_lf.collect())
    assert "_is_processed" in result_df.columns
    assert all(result_df["_is_processed"])


def test_transformer_version_default():
    """
    GIVEN a transformer instance
    WHEN the version property is accessed
    THEN it should return '1.0.0' by default for metadata tracking
    """
    transformer = MockTransformer(dataset_id="ds", job_id="j")
    assert transformer.version == "1.0.0"


def test_transformer_apply_empty_dataset():
    """
    GIVEN an empty LazyFrame with a schema
    WHEN apply is called
    THEN it should still return a valid (empty) LazyFrame with the correct schema
    """
    transformer = MockTransformer(dataset_id="ds", job_id="j")
    ctx = MagicMock()

    df = pl.DataFrame({"id": []}, schema={"id": pl.Int64})
    result_df = cast("pl.DataFrame", transformer.apply(df.lazy(), ctx).collect())

    assert result_df.height == 0
    assert "_is_processed" in result_df.columns
