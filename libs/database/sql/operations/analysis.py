from typing import TYPE_CHECKING, Any

from .base import SQLOperation, format_table_ref, register_compile_func

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


@register_compile_func(SQLOperation.SELECT)
def _compile_select(
    compiler: "SQLCompiler",
    table: str,
    fields: list[str] | str = "*",
    where_cond: Any | None = None,
    # limit: int | None = None,
    **kwargs,
) -> str:
    """
    Compiles a SELECT query using YAML base template and dynamic WHERE generation.
    """
    # 1. Format Fields
    if isinstance(fields, list | tuple | set):
        fields_str = ", ".join([compiler.quote_identifier(f) for f in fields])
    else:
        fields_str = fields.strip() if fields else "*"

    # 2. Format Table Identifier
    formatted_table = format_table_ref(compiler, table)

    # 3. Get Base YAML Template (e.g. "select {fields} from {table}")
    template_str = compiler.template.analysis.get(
        "select", "select {fields} from {table}"
    )

    # Strip any hardcoded 'where {where_cond}' if left over in old YAMLs
    template_str = template_str.split("where")[0].strip()

    sql = template_str.format(fields=fields_str, table=formatted_table)

    # 4. Inject WHERE Clause via compile_conditions
    if where_cond:
        where_clause = compiler.compile_conditions(where_cond)
        if where_clause:
            sql = f"{sql} {where_clause}"

    # # 5. Append LIMIT if specified
    # if limit is not None:
    #     sql = f"{sql} limit {limit}"

    return sql


@register_compile_func(SQLOperation.COUNT)
def _compile_count(
    compiler: "SQLCompiler",
    table: str,
    where_cond: Any | None = None,
    alias: str = "cnt",
    **kwargs,
) -> str:
    """
    Compiles a COUNT query using YAML base template or dynamic fallback.
    Supports physical tables as well as subqueries.
    """
    # 1. Safely format table reference (wraps SELECT queries in subqueries)
    formatted_table = format_table_ref(compiler, table)

    # 2. Get base template from YAML (e.g. "select count(*) as cnt from {table}")
    template_str = compiler.template.analysis.get(
        "count", "select count(*) as {alias} from {table}"
    )

    # Clean out any legacy static 'where' clauses in the template string
    template_str = template_str.split("where")[0].strip()

    # If the template contains `{alias}`, pass it; otherwise format standard `{table}`
    try:
        sql = template_str.format(
            table=formatted_table, alias=compiler.quote_identifier(alias)
        )
    except KeyError:
        sql = template_str.format(table=formatted_table)

    # 3. Append dynamic WHERE conditions using compile_conditions
    if where_cond:
        where_clause = compiler.compile_conditions(where_cond)
        if where_clause:
            sql = f"{sql} {where_clause}"

    return sql


@register_compile_func(SQLOperation.MINUS)
def _compile_minus(
    compiler: "SQLCompiler",
    ref_table: str,
    other_table: str,
    fields: list[str] | str = "*",
    **kwargs,
) -> str:
    """
    Compiles a set difference query comparing ref_table against other_table.
    Supports physical tables and raw subquery inputs for both sides.
    """
    # 1. Format column projection
    if isinstance(fields, list | tuple | set):
        fields_str = ", ".join([compiler.quote_identifier(c) for c in fields])
    else:
        fields_str = fields.strip() if fields else "*"

    # 2. Guard table/subquery references safely
    formatted_ref = format_table_ref(compiler, ref_table, alias="ref_src")
    formatted_other = format_table_ref(compiler, other_table, alias="other_src")

    # 3. Retrieve YAML template from analysis section
    template_str = compiler.template.analysis.get("minus")

    if not template_str:
        # Fallback ANSI EXCEPT query syntax
        return (
            f"SELECT {fields_str} FROM {formatted_ref} "
            f"EXCEPT "
            f"SELECT {fields_str} FROM {formatted_other}"
        )

    return template_str.format(
        fields=fields_str,
        ref_table=formatted_ref,
        other_table=formatted_other,
    )
