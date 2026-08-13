from typing import TYPE_CHECKING

from .base import SQLOperation, register_compile_func

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


@register_compile_func(SQLOperation.EXIST)
def _compile_table_exists(
    compiler: "SQLCompiler",
    table_name: str,
    default_schema: str = "default",
    **kwargs,
) -> str:
    """
    Compiles a query to check if a table exists in the target database catalog.
    """
    # 1. Parse table parts (e.g. "analytics.users" -> schema="analytics", table="users")
    components = compiler.validate_identifier(table_name)
    if components:
        schema = (
            components.get("schema") or components.get("database") or default_schema
        )
        table = components.get("table") or table_name

    # 2. Get template from metadata section of dialect YAML
    template_str = compiler.template.metadata.get("table_exists")

    if not template_str:
        # Generic ANSI SQL fallback for catalog checking
        return (
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
        )

    # 3. Format catalog lookup template
    return template_str.format(
        schema=schema,
        table=table,
        db=schema,
    )


@register_compile_func(SQLOperation.COLUMNS)
def _compile_get_columns(
    compiler: "SQLCompiler",
    fq_table: str,
    **kwargs,
) -> str:
    """
    Compiles a query to retrieve column metadata for a target table.
    """
    # 1. Parse table parts (e.g. "analytics.users" -> schema="analytics", table="users")
    components = compiler.validate_identifier(fq_table) or {}
    if components:
        db = components.get("database")
        schema = components.get("schema")
        table = components.get("table")

    # 2. Get template from metadata section of dialect YAML
    template_str = compiler.template.metadata.get("columns")

    if not template_str:
        # Generic ANSI SQL fallback for information_schema.columns
        return (
            f"SELECT column_name, data_type, character_maximum_length, "
            f"numeric_precision, numeric_scale, is_nullable "
            f"FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
            f"ORDER BY ordinal_position"
        )

    # 3. Format column lookup template
    return template_str.format(
        db=db or schema,
        schema=schema,
        table=table,
    )
