from typing import TYPE_CHECKING, Any

from .base import SQLOperation, format_fields, format_table_ref, register_compile_func

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


@register_compile_func(SQLOperation.MERGE_UPSERT)
def _compile_merge_upsert(
    compiler: "SQLCompiler",
    tgt_table: str,
    src_table: str,
    join_cond: str,
    fields: list[str] | str = "*",
    **kwargs,
) -> str:
    """
    Compiles an upsert operation using the dialect's merge strategy.
    """
    formatted_tgt = format_table_ref(compiler, tgt_table, alias="tgt")
    formatted_src = format_table_ref(compiler, src_table, alias="src")
    formatted_fields = format_fields(compiler, fields)

    # Fetch template from the merge section
    template_str = compiler.template.merge.get("merge_upsert")

    if not template_str:
        # Fallback ANSI-like multi-statement strategy
        template_str = (
            "ALTER TABLE {tgt_table} DELETE WHERE {join_cond};\n"
            "INSERT INTO {tgt_table} ({insert_fields})\n"
            "SELECT {src_fields} FROM {src_table}"
        )

    return template_str.format(
        tgt_table=formatted_tgt,
        src_table=formatted_src,
        join_cond=join_cond,
        insert_fields=formatted_fields,
        src_fields=formatted_fields,
    )


@register_compile_func(SQLOperation.MERGE_INSERT)
def _compile_merge_insert(
    compiler: "SQLCompiler",
    tgt_table: str,
    src_table: str,
    fields: list[str] | str = "*",
    where_cond: Any | None = None,
    **kwargs,
) -> str:
    """
    Compiles a merge insert operation.
    """
    formatted_tgt = format_table_ref(compiler, tgt_table, alias="tgt")
    formatted_src = format_table_ref(compiler, src_table, alias="src")
    formatted_fields = format_fields(compiler, fields)

    template_str = compiler.template.merge.get("merge_insert")

    if not template_str:
        template_str = (
            "INSERT INTO {tgt_table} ({insert_fields})\n"
            "SELECT {src_fields} FROM {src_table}"
        )

    sql = template_str.format(
        tgt_table=formatted_tgt,
        src_table=formatted_src,
        insert_fields=formatted_fields,
        src_fields=formatted_fields,
    )

    # Append optional filter conditions on source rows if supplied
    if where_cond:
        where_clause = compiler.compile_conditions(where_cond)
        if where_clause:
            sql = f"{sql}\n{where_clause}"

    return sql


@register_compile_func(SQLOperation.MERGE_DELETE)
def _compile_merge_delete(
    compiler: "SQLCompiler",
    tgt_table: str,
    where_cond: Any | str = None,
    **kwargs,
) -> str:
    """
    Compiles a delete operation on the target table.
    """
    formatted_tgt = format_table_ref(compiler, tgt_table, alias="tgt")

    # Format where condition using compile_conditions if passed as expression/dict/list
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

    template_str = compiler.template.merge.get("merge_delete")

    if not template_str:
        template_str = "DELETE FROM {tgt_table}\n{where_cond}"

    return template_str.format(
        tgt_table=formatted_tgt,
        where_cond=where_clause,
    ).strip()
