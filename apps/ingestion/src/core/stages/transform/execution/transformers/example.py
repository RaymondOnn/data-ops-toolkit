"""Built-in transformer implementations."""

import polars as pl
from loguru import logger

from src.core.stages.transform.execution.base import (
    DistributedTransformer,
    TransformContext,
)
from src.core.stages.transform.execution.factory import TransformFactory

LOG = logger


@TransformFactory.register("masking")
class PIIMaskingTransformer(DistributedTransformer):
    """
    An example transformer that masks a specific column (e.g., 'email' or 'ssn')
    using PyArrow compute functions for speed.
    """

    def __init__(self, context: TransformContext):
        self.fields_to_mask = context.sub_step.params["fields_to_mask"]
        self.mask_char = context.sub_step.params["mask_char"]

    def process_frame(self, df: pl.LazyFrame) -> pl.LazyFrame:
        mask_exprs = [
            pl.when(pl.col(col).is_not_null())
            .then(pl.lit(self.mask_char * 8))
            .otherwise(None)
            .alias(col)
            for col in self.fields_to_mask
        ]
        return df.with_columns(mask_exprs)
