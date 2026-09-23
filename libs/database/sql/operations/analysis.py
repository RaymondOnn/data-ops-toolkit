"""Analysis SQL operation compilation functions."""

from typing import TYPE_CHECKING, Any

from libs.database.sql.exceptions import SQLCompilationError

from .base import (
    SQLOperation,
    SQLOperationType,
)
from .utils import (
    append_where_clause,
    format_fields,
    format_table_ref,
)

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


@SQLOperation.register(SQLOperationType.SELECT)
def _compile_select(
    compiler: "SQLCompiler",
    table: str,
    fields: list[str] | str = "*",
    where_cond: Any | None = None,
    **kwargs,
) -> str:
    """Compiles a SELECT query using YAML base template and dynamic WHERE generation."""
    fields_str = format_fields(compiler, fields)
    formatted_table = format_table_ref(compiler, table)

    template_str = compiler.template.analysis.get("select")
    if not template_str:
        raise SQLCompilationError(
            f"No 'select' template configured for dialect '{compiler.dialect}'."
        )

    template_str = template_str.split("where")[0].strip()
    sql = template_str.format(fields=fields_str, table=formatted_table)
    return append_where_clause(compiler, sql, where_cond)


@SQLOperation.register(SQLOperationType.COUNT)
def _compile_count(
    compiler: "SQLCompiler",
    table: str,
    where_cond: Any | None = None,
    alias: str = "cnt",
    **kwargs,
) -> str:
    """Compiles a COUNT query using YAML base template or dynamic fallback."""
    formatted_table = format_table_ref(compiler, table)

    template_str = compiler.template.analysis.get("count")
    if not template_str:
        raise SQLCompilationError(
            f"No 'count' template configured for dialect '{compiler.dialect}'."
        )

    template_str = template_str.split("where")[0].strip()

    try:
        sql = template_str.format(
            table=formatted_table, alias=compiler.quote_identifier(alias)
        )
    except KeyError:
        sql = template_str.format(table=formatted_table)

    return append_where_clause(compiler, sql, where_cond)


@SQLOperation.register(SQLOperationType.MINUS)
def _compile_minus(
    compiler: "SQLCompiler",
    ref_table: str,
    other_table: str,
    fields: list[str] | str = "*",
    **kwargs,
) -> str:
    """Compiles a set difference query comparing ref_table against other_table."""
    template_str = compiler.template.analysis.get("minus")
    if not template_str:
        raise SQLCompilationError(
            f"No 'minus' template configured for dialect '{compiler.dialect}'."
        )

    fields_str = format_fields(compiler, fields)
    formatted_ref = format_table_ref(compiler, ref_table, alias="ref_src")
    formatted_other = format_table_ref(compiler, other_table, alias="other_src")

    return template_str.format(
        fields=fields_str,
        ref_table=formatted_ref,
        other_table=formatted_other,
    )
