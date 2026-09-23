import re
from typing import TYPE_CHECKING, Any

from libs.database.sql.compile import ConfigurationValidationError
from libs.database.sql.utils import format_sql_value

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


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


def format_fields(
    compiler: "SQLCompiler",
    fields: list[str] | tuple[str, ...] | set[str] | str | None,
) -> str:
    """Formats string or collection of column names cleanly into a quoted SQL projection list."""
    if not fields or fields == "*":
        return "*"
    if isinstance(fields, list | tuple | set):
        return ", ".join(compiler.quote_identifier(f) for f in fields)
    return fields.strip()


def parse_table_components(
    compiler: "SQLCompiler",
    table_name: str,
    default_schema: str = "default",
) -> dict[str, str]:
    """Extracts database catalog identifiers (db, schema, table) into a lookup mapping."""
    components = compiler.validate_identifier(table_name) or {}
    schema = components.get("schema") or components.get("database") or default_schema
    table = components.get("table") or table_name
    db = components.get("database") or schema
    return {"db": db, "schema": schema, "table": table}


def append_where_clause(
    compiler: "SQLCompiler",
    sql: str,
    where_cond: str | None,
    clause: str = "WHERE",
    newline: bool = False,
) -> str:
    """Compiles and appends optional WHERE/AND filtering clauses to a base SQL query."""
    if not where_cond:
        return sql
    where_str = compiler.compile_conditions(where_cond, clause=clause)
    if not where_str:
        return sql
    sep = "\n" if newline else " "
    return f"{sql}{sep}{where_str}"


def format_join_cond(
    compiler: "SQLCompiler",
    join_cond: list[tuple[str, str]] | list[str] | str,
) -> str:
    """Formats join condition into a SQL string."""
    if isinstance(join_cond, str):
        return join_cond

    if isinstance(join_cond, list):
        join_pairs = []
        for item in join_cond:
            if isinstance(item, tuple):
                tgt_col, src_col = item
            else:
                tgt_col = src_col = item

            q_tgt = compiler.quote_identifier(tgt_col)
            q_src = compiler.quote_identifier(src_col)
            join_pairs.append(f"tgt.{q_tgt} = src.{q_src}")

        return " AND ".join(join_pairs)

    raise TypeError("join_cond must be a list of keys/tuples or a SQL string.")


def format_set_values(
    compiler: "SQLCompiler",
    set_values: dict[str, Any] | list[str] | str | None,
    fields: list[str] | str | None = None,
) -> str:
    """Formats the SET clause for an UPDATE statement."""
    if isinstance(set_values, dict):
        return ", ".join(
            f"{compiler.quote_identifier(k)} = {format_sql_value(v)}"
            for k, v in set_values.items()
        )

    if isinstance(set_values, list):
        return ", ".join(
            f"{compiler.quote_identifier(f)} = src.{compiler.quote_identifier(f)}"
            for f in set_values
        )

    if isinstance(set_values, str):
        return set_values

    if fields:
        field_list = (
            fields
            if isinstance(fields, list)
            else [f.strip() for f in fields.split(",") if f.strip() != "*"]
        )
        return ", ".join(
            f"{compiler.quote_identifier(f)} = src.{compiler.quote_identifier(f)}"
            for f in field_list
        )

    return ""
