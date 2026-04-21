def get_fully_qualified_table(
    database: str | None = None, schema: str | None = None, table: str | None = None
) -> str:
    """Utility to construct fully qualified table names for various databases."""
    if not table:
        raise ValueError("Table name is required")
    parts = [part for part in [database, schema, table] if part]
    return ".".join(parts)
