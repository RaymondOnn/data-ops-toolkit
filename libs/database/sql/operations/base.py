import re
from collections.abc import Callable
from enum import StrEnum
from typing import TYPE_CHECKING

from libs.database.sql.compile import ConfigurationValidationError

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


class SQLOperation(StrEnum):
    # core
    CREATE_TABLE = "create_table"
    CREATE_SCHEMA = "create_schema"
    TRUNCATE = "truncate"
    INSERT = "insert"
    UPDATE = "update"

    DROP_TABLE = "drop_table"
    DROP_VIEW = "drop_view"
    DROP_SCHEMA = "drop_schema"
    DESCRIBE_TABLE = "describe_table"
    LIKE_TABLE = "like_table"
    CLONE_TABLE = "clone_table"

    # merge
    UPSERT = "upsert"
    MERGE_INSERT = "merge_insert"
    MERGE_DELETE = "merge_delete"
    MERGE_UPSERT = "merge_upsert"

    # metadata
    EXIST = "table_exists"
    COLUMNS = "columns"

    # analysis
    SELECT = "select"
    COUNT = "count"
    MINUS = "minus"


# Central Compilation Registry Map
COMPILE_FUNCTIONS: dict[SQLOperation | str, Callable] = {}


def register_compile_func(op_type: SQLOperation | str):
    """Decorator to register compilation routines with the strategy engine."""

    def decorator(func: Callable):
        COMPILE_FUNCTIONS[op_type] = func
        return func

    return decorator


def format_table_ref(
    compiler: "SQLCompiler",
    table_or_query: str,
    alias: str = "src",
    enforce_qualified: bool = False,
) -> str:
    """
    Guards table inputs. If given a full SELECT query, wraps it in parentheses
    with an alias. If given a plain table identifier, returns the quoted name.

    Examples:
        "analytics.users" -> '"analytics"."users"'
        "SELECT id, name FROM users" -> '(SELECT id, name FROM users) AS src'
        "(SELECT * FROM users)" -> '(SELECT * FROM users) AS src'
    """
    if not table_or_query:
        return ""

    cleaned = table_or_query.strip()

    # Check if input is a subquery
    if re.match(r"^\s*\(?\s*select\b", cleaned, re.IGNORECASE):
        if not (cleaned.startswith("(") and cleaned.endswith(")")):
            cleaned = f"({cleaned})"
        return f"{cleaned} AS {compiler.quote_identifier(alias)}" if alias else cleaned

    # Table identifier validation check
    parts = cleaned.split(".")
    if enforce_qualified and len(parts) < 2:
        raise ConfigurationValidationError(
            f"Table identifier '{cleaned}' is not fully qualified. "
            f"Expected '[schema].[table]', but got depth of {len(parts)}."
        )

    return compiler.quote_identifier(cleaned)


def format_fields(compiler: "SQLCompiler", fields: list[str] | str) -> str:
    """Helper to format string or list of column names cleanly."""
    if isinstance(fields, list | tuple | set):
        return ", ".join([compiler.quote_identifier(f) for f in fields])
    return fields.strip() if fields else "*"
