from typing import Any


def get_fully_qualified_table(
    database: str | None = None, schema: str | None = None, table: str | None = None
) -> str:
    """Utility to construct fully qualified table names for various databases."""
    if not table:
        raise ValueError("Table name is required")
    parts = [part for part in [database, schema, table] if part]
    return ".".join(parts)


def build_select_query(
    table: str, columns: list | None = None, where: dict[str, Any] | None = None
) -> tuple:
    """
    Builds a parameterized SQL SELECT query.
    """
    # 1. Handle columns
    cols_str = ", ".join(columns) if columns else "*"

    # 2. Base query
    sql = f"SELECT {cols_str} FROM {table}"
    params = []

    # 3. Handle WHERE clause dynamically
    if where:
        where_clauses = []
        for key, value in where.items():
            # Use ? as placeholders for SQLite (use %s for PostgreSQL/MySQL)
            where_clauses.append(f"{key} = ?")
            params.append(value)

        sql += " WHERE " + " AND ".join(where_clauses)

    return sql, params
