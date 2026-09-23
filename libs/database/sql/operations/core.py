"""Core SQL operation compilation functions."""

from typing import TYPE_CHECKING, Any

import polars as pl

from libs.database.sql.compile import SQLCompilationError

from .base import (
    SQLOperation,
    SQLOperationType,
)
from .utils import (
    format_fields,
    format_set_values,
    parse_table_components,
)

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


@SQLOperation.register(SQLOperationType.TRUNCATE)
def _compile_truncate(
    compiler: "SQLCompiler",
    table_name: str,
    **kwargs,
) -> str:
    """Compiles a TRUNCATE TABLE query."""
    template_str = compiler.template.core.get("truncate")
    if not template_str:
        raise SQLCompilationError(
            f"No 'truncate_table' template configured for dialect '{compiler.dialect}'."
        )

    quoted_table = compiler.quote_identifier(table_name)
    return template_str.format(table=quoted_table)


@SQLOperation.register(SQLOperationType.UPDATE)
def _compile_update(
    compiler: "SQLCompiler",
    table_name: str,
    set_values: dict[str, Any] | str,
    fields: list[str] | str | None = None,
    where_cond: str | None = None,
    **kwargs,
) -> str:
    """Compiles an UPDATE statement."""
    template_str = compiler.template.core.get("update")
    if not template_str:
        raise SQLCompilationError(
            f"No 'update' template configured for dialect '{compiler.dialect}'."
        )

    quoted_table = compiler.quote_identifier(table_name)
    formatted_set = format_set_values(compiler, set_values, fields=fields)

    if not formatted_set:
        raise SQLCompilationError(
            "Cannot compile UPDATE statement without SET assignment values."
        )

    where_clause = where_cond if where_cond else "1=1"

    return template_str.format(
        table=quoted_table,
        set_values=formatted_set,
        where_cond=where_clause,
        join_cond=where_clause,
    )


def _format_literal(val: Any) -> str:
    """Formats a Python literal value safely for SQL INSERT statements."""
    if val is None:
        return "NULL"
    if isinstance(val, str):
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    return str(val)


@SQLOperation.register(SQLOperationType.INSERT)
def _compile_insert(
    compiler: "SQLCompiler",
    table_name: str,
    records: list[dict[str, Any]] | pl.DataFrame | None = None,
    select_query: str | None = None,
    target_columns: list[str] | str | None = None,
    **kwargs,
) -> str:
    """Compiles an INSERT query supporting dictionaries, DataFrames, or SELECT subqueries."""
    quoted_table = compiler.quote_identifier(table_name)

    if isinstance(records, pl.DataFrame):
        records = records.to_dicts()

    if records:
        if not isinstance(records, list):
            records = [records]

        cols = list(records[0].keys())
        quoted_cols = format_fields(compiler, cols)

        row_tuples = [
            f"({', '.join(_format_literal(r.get(c)) for c in cols)})" for r in records
        ]

        values_clause = ", ".join(row_tuples)
        return f"INSERT INTO {quoted_table} ({quoted_cols}) VALUES {values_clause}"

    if select_query:
        cleaned_select = select_query.strip()
        if target_columns:
            formatted_cols = format_fields(compiler, target_columns)
            return f"INSERT INTO {quoted_table} ({formatted_cols}) {cleaned_select}"
        return f"INSERT INTO {quoted_table} {cleaned_select}"

    raise SQLCompilationError(
        "Cannot compile INSERT statement without providing either 'records' or 'select_query'."
    )


@SQLOperation.register(SQLOperationType.CREATE_TABLE)
def _compile_create_table(
    compiler: "SQLCompiler",
    table_name: str,
    schema_columns: dict[str, str],
    **kwargs,
) -> str:
    """Compiles a CREATE TABLE statement."""
    template_str = compiler.template.core.get("create_table")
    if not template_str:
        raise SQLCompilationError(
            f"No 'create_table' template configured for dialect '{compiler.dialect}'"
        )

    if not schema_columns:
        raise SQLCompilationError(
            "Cannot compile CREATE TABLE without column definitions."
        )

    quoted_table = compiler.quote_identifier(table_name)
    col_declarations = [
        f"{compiler.quote_identifier(col)} {compiler._map_generic_type(type_expr)}"
        for col, type_expr in schema_columns.items()
    ]
    col_types_str = ", ".join(col_declarations)

    return template_str.format(table=quoted_table, col_types=col_types_str)


@SQLOperation.register(SQLOperationType.DROP_TABLE)
def _compile_drop_table(compiler: "SQLCompiler", table_name: str, **kwargs) -> str:
    """Compiles a DROP TABLE query."""
    template_str = compiler.template.core.get("drop_table")
    if not template_str:
        raise SQLCompilationError(
            f"No 'drop_table' template configured for dialect '{compiler.dialect}'."
        )

    quoted_table = compiler.quote_identifier(table_name)
    return template_str.format(table=quoted_table)


@SQLOperation.register(SQLOperationType.DESCRIBE_TABLE)
def _compile_describe_table(
    compiler: "SQLCompiler",
    table_name: str,
    default_schema: str = "default",
    **kwargs,
) -> str:
    """Compiles a query to describe/inspect table structure and schema metadata."""
    template_str = compiler.template.core.get("describe_table")
    if not template_str:
        raise SQLCompilationError(
            f"No 'describe_table' template configured for dialect '{compiler.dialect}'."
        )

    comp = parse_table_components(compiler, table_name, default_schema)
    return template_str.format(**comp)


@SQLOperation.register(SQLOperationType.LIKE_TABLE)
def _compile_like_table(
    compiler: "SQLCompiler",
    tgt_table: str,
    src_table: str,
    **kwargs,
) -> str:
    """Compiles a schema-only table clone query (copies structure, no rows)."""
    template_str = compiler.template.core.get("like_table")
    if not template_str:
        raise SQLCompilationError(
            f"No 'like_table' template configured for dialect '{compiler.dialect}'."
        )

    quoted_tgt = compiler.quote_identifier(tgt_table)
    quoted_src = compiler.quote_identifier(src_table)

    return template_str.format(tgt_table=quoted_tgt, src_table=quoted_src)


@SQLOperation.register(SQLOperationType.CLONE_TABLE)
def _compile_clone_table(
    compiler: "SQLCompiler",
    tgt_table: str,
    src_table: str,
    **kwargs,
) -> str:
    """Compiles a full table clone query (copies both structure AND data)."""
    template_str = compiler.template.core.get("clone_table")
    if not template_str:
        raise SQLCompilationError(
            f"No 'clone_table' template configured for dialect '{compiler.dialect}'."
        )

    quoted_tgt = compiler.quote_identifier(tgt_table)
    quoted_src = compiler.quote_identifier(src_table)

    return template_str.format(tgt_table=quoted_tgt, src_table=quoted_src)


@SQLOperation.register(SQLOperationType.ADD_COLUMN)
def _compile_add_column(
    compiler: "SQLCompiler",
    table_name: str,
    column_name: str,
    data_type: str,
    **kwargs,
) -> str:
    """Compiles an ALTER TABLE ADD COLUMN statement."""
    template_str = compiler.template.core.get("add_column")
    if not template_str:
        raise SQLCompilationError(
            f"No 'add_column' template configured for dialect '{compiler.dialect}'."
        )

    quoted_table = compiler.quote_identifier(table_name)
    quoted_col = compiler.quote_identifier(column_name)
    mapped_type = compiler._map_generic_type(data_type)

    return template_str.format(
        table=quoted_table,
        column_name=quoted_col,
        data_type=mapped_type,
    )
