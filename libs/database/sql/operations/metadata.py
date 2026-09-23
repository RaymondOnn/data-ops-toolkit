"""Metadata SQL operation compilation functions."""

from typing import TYPE_CHECKING

from libs.database.sql.exceptions import SQLCompilationError

from .base import SQLOperation, SQLOperationType
from .utils import parse_table_components

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


@SQLOperation.register(SQLOperationType.EXIST)
def _compile_table_exists(
    compiler: "SQLCompiler",
    table_name: str,
    default_schema: str = "default",
    **kwargs,
) -> str:
    """Compiles a query to check if a table exists in the target database catalog."""
    comp = parse_table_components(compiler, table_name, default_schema)
    template_str = compiler.template.metadata.get("table_exists")

    if not template_str:
        raise SQLCompilationError(
            f"No 'exist' template configured for dialect '{compiler.dialect}'."
        )

    return template_str.format(**comp)


@SQLOperation.register(SQLOperationType.COLUMNS)
def _compile_get_columns(
    compiler: "SQLCompiler",
    fq_table: str,
    **kwargs,
) -> str:
    """Compiles a query to retrieve column metadata for a target table."""
    comp = parse_table_components(compiler, fq_table)
    template_str = compiler.template.metadata.get("columns")

    if not template_str:
        raise SQLCompilationError(
            f"No 'columns' template configured for dialect '{compiler.dialect}'."
        )

    return template_str.format(**comp)
