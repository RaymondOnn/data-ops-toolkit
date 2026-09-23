import polars as pl


def merge_schemas(
    schemas: list[dict[str, pl.DataType] | pl.Schema],
) -> dict[str, pl.DataType]:
    """Merges multiple partition schemas into a unified schema using relaxed type matching."""
    if not schemas:
        return {}

    # Create empty DataFrames to utilize Polars relaxed diagonal concatenation for schema merging
    empty_dfs = [pl.DataFrame(schema=s) for s in schemas]
    merged_schema = pl.concat(empty_dfs, how="diagonal_relaxed").schema

    # Explicitly ensure returning dict[str, pl.DataType]
    return dict(merged_schema)
