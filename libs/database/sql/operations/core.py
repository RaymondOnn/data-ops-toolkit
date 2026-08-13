from typing import TYPE_CHECKING

from libs.database.sql.compile import SQLCompilationError

from .base import SQLOperation, register_compile_func

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


@register_compile_func(SQLOperation.CREATE_TABLE)
def _compile_create_table(
    compiler: "SQLCompiler", table_name: str, schema_columns: dict[str, str], **kwargs
) -> str:
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

    template_str = compiler.template.core.get("create_table")
    if not template_str:
        raise SQLCompilationError(
            f"No 'create_table' template configured for dialect '{compiler.dialect}'"
        )

    return template_str.format(table=quoted_table, col_types=col_types_str)


@register_compile_func(SQLOperation.DROP_TABLE)
def _compile_drop_table(compiler: "SQLCompiler", table_name: str, **kwargs) -> str:
    quoted_table = compiler.quote_identifier(table_name)
    template_str = compiler.template.core.get(
        "drop_table", "drop table if exists {table}"
    )
    return template_str.format(table=quoted_table)


@register_compile_func(SQLOperation.DESCRIBE_TABLE)
def _compile_describe_table(
    compiler: "SQLCompiler",
    table_name: str,
    default_schema: str = "default",
    **kwargs,
) -> str:
    """
    Compiles a query to describe/inspect table structure and schema metadata.
    """
    # 1. Parse table hierarchy components
    components = compiler.validate_identifier(table_name)
    if components:
        schema = (
            components.get("schema") or components.get("database") or default_schema
        )
        table = components.get("table") or table_name

    # 2. Extract YAML template from core section
    template_str = compiler.template.core.get("describe_table")

    if not template_str:
        # Standard ANSI SQL fallback
        return f"DESCRIBE TABLE {compiler.quote_identifier(table_name)}"

    return template_str.format(
        schema=schema,
        table=table,
        db=schema,
    )


@register_compile_func(SQLOperation.LIKE_TABLE)
def _compile_like_table(
    compiler: "SQLCompiler",
    tgt_table: str,
    src_table: str,
    **kwargs,
) -> str:
    """
    Compiles a schema-only table clone query (copies structure, no rows).
    """
    quoted_tgt = compiler.quote_identifier(tgt_table)
    quoted_src = compiler.quote_identifier(src_table)

    # 1. Check YAML template (e.g. "create or replace table {tgt_table} as {src_table}")
    template_str = compiler.template.core.get("clone_schema")

    if not template_str:
        # Fallback to standard ANSI SQL "WHERE 1=0" structural clone
        return f"CREATE TABLE {quoted_tgt} AS SELECT * FROM {quoted_src} WHERE 1=0"

    return template_str.format(tgt_table=quoted_tgt, src_table=quoted_src)


@register_compile_func(SQLOperation.CLONE_TABLE)
def _compile_clone_table(
    compiler: "SQLCompiler",
    tgt_table: str,
    src_table: str,
    **kwargs,
) -> str:
    """
    Compiles a full table clone query (copies both structure AND data).
    """
    quoted_tgt = compiler.quote_identifier(tgt_table)
    quoted_src = compiler.quote_identifier(src_table)

    # 1. Check YAML template (e.g. "create or replace table {tgt_table} clone as {src_table}")
    template_str = compiler.template.core.get("clone_full")

    if not template_str:
        # Fallback ANSI CTAS data clone
        return f"CREATE TABLE {quoted_tgt} AS SELECT * FROM {quoted_src}"

    return template_str.format(tgt_table=quoted_tgt, src_table=quoted_src)
