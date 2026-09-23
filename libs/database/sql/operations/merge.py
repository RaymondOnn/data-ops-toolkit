"""Merge SQL operation compilation functions."""

from typing import TYPE_CHECKING, Any

from libs.database.sql.exceptions import SQLCompilationError

from .base import (
    SQLOperation,
    SQLOperationType,
)
from .utils import (
    append_where_clause,
    format_fields,
    format_join_cond,
    format_set_values,
    format_table_ref,
)

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


@SQLOperation.register(SQLOperationType.MERGE_UPSERT)
def _compile_merge_upsert(
    compiler: "SQLCompiler",
    tgt_table: str,
    src_table: str,
    join_cond: str,
    fields: list[str] | str = "*",
    **kwargs,
) -> str:
    """Compiles an upsert operation using the dialect's merge strategy."""
    template_str = compiler.template.merge.get("merge_upsert")
    if not template_str:
        raise SQLCompilationError(
            f"No 'merge_upsert' template configured for dialect '{compiler.dialect}'."
        )

    formatted_tgt = format_table_ref(compiler, tgt_table, alias="tgt")
    formatted_src = format_table_ref(compiler, src_table, alias="src")
    formatted_fields = format_fields(compiler, fields)

    return template_str.format(
        tgt_table=formatted_tgt,
        src_table=formatted_src,
        join_cond=join_cond,
        insert_fields=formatted_fields,
        src_fields=formatted_fields,
    )


@SQLOperation.register(SQLOperationType.MERGE_INSERT)
def _compile_merge_insert(
    compiler: "SQLCompiler",
    tgt_table: str,
    src_table: str,
    fields: list[str] | str = "*",
    where_cond: Any | None = None,
    **kwargs,
) -> str:
    """Compiles a merge insert operation."""
    template_str = compiler.template.merge.get("merge_insert")
    if not template_str:
        raise SQLCompilationError(
            f"No 'merge_insert' template configured for dialect '{compiler.dialect}'."
        )

    formatted_tgt = format_table_ref(compiler, tgt_table, alias="tgt")
    formatted_src = format_table_ref(compiler, src_table, alias="src")
    formatted_fields = format_fields(compiler, fields)

    sql = template_str.format(
        tgt_table=formatted_tgt,
        src_table=formatted_src,
        insert_fields=formatted_fields,
        src_fields=formatted_fields,
    )

    return append_where_clause(compiler, sql, where_cond, newline=True)


@SQLOperation.register(SQLOperationType.MERGE_DELETE)
def _compile_merge_delete(
    compiler: "SQLCompiler",
    tgt_table: str,
    where_cond: Any | str = None,
    **kwargs,
) -> str:
    """Compiles a delete operation on the target table."""
    template_str = compiler.template.merge.get("merge_delete")
    if not template_str:
        raise SQLCompilationError(
            f"No 'merge_delete' template configured for dialect '{compiler.dialect}'."
        )

    formatted_tgt = format_table_ref(compiler, tgt_table, alias="tgt")
    if where_cond and not isinstance(where_cond, str):
        where_clause = compiler.compile_conditions(where_cond)
    elif isinstance(where_cond, str):
        where_clause = (
            f"WHERE {where_cond}"
            if not where_cond.upper().startswith("WHERE")
            else where_cond
        )
    else:
        where_clause = ""

    return template_str.format(
        tgt_table=formatted_tgt,
        where_cond=where_clause,
    ).strip()


@SQLOperation.register(SQLOperationType.MERGE_UPDATE)
def _compile_merge_update(
    compiler: "SQLCompiler",
    tgt_table: str,
    src_table: str,
    join_cond: list[tuple[str, str]] | list[str] | str,
    set_values: dict[str, str] | list[str] | str | None = None,
    *,
    fields: list[str] | str | None = None,
    where_cond: Any | None = None,
    **kwargs,
) -> str:
    """Compiles a merge update operation to update matching rows in target table from source table."""
    template_str = compiler.template.merge.get("merge_update")
    if not template_str:
        raise SQLCompilationError(
            f"No 'merge_update' template configured for dialect '{compiler.dialect}'."
        )

    formatted_tgt = format_table_ref(compiler, tgt_table, alias="tgt")
    formatted_src = format_table_ref(compiler, src_table, alias="src")
    formatted_join = format_join_cond(compiler, join_cond)
    formatted_set = format_set_values(compiler, set_values, fields)

    sql = template_str.format(
        tgt_table=formatted_tgt,
        src_table=formatted_src,
        join_cond=formatted_join,
        set_values=formatted_set,
    ).strip()

    return append_where_clause(compiler, sql, where_cond, clause="AND", newline=True)
