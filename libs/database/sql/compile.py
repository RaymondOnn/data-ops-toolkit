import logging
import re
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar, Union

import msgspec
from msgspec import Struct, field

from libs.utils.dict import deep_merge

from .enums import JoinConfig, SelectQueryContext, SQLContext
from .exceptions import ConfigurationValidationError, SQLCompilationError
from .operations.base import COMPILE_FUNCTIONS, SQLOperation

LOG = logging.getLogger(__name__)
DB_TEMPLATE_DIR = Path(__file__).parent / "templates"

T = TypeVar("T")
Predicate = Union[  # noqa
    str,
    dict[str, Any],
    tuple[str, str, Any],
    tuple[str, Sequence[Any]],
    tuple[str, str],
]


class Dialect(StrEnum):
    DEFAULT = "ansi"
    CLICKHOUSE = "clickhouse"
    POSTGRES = "postgres"
    DUCKDB = "duckdb"
    ORACLE = "oracle"
    SNOWFLAKE = "snowflake"


class DialectTemplate(Struct):
    identifiers: dict[str, Any] = field(default_factory=dict)
    core: dict[str, Any] = field(default_factory=dict)
    merge: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    analysis: dict[str, Any] = field(default_factory=dict)
    function: dict[str, Any] = field(default_factory=dict)
    general_type_map: dict[str, Any] = field(default_factory=dict)
    native_type_map: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def read_yaml(path: str, type_: type[T] = dict) -> T:  # type: ignore
        with Path(path).open("rb") as f:
            return msgspec.yaml.decode(f.read(), type=type_)

    @classmethod
    def build(cls, dialect: Dialect, overrides: dict[str, Any] | None = None):
        template_path = DB_TEMPLATE_DIR / f"{dialect.value}.yaml"
        template_dict: dict[str, Any] = cls.read_yaml(path=str(template_path))
        overrides = overrides or {}

        if overrides:
            LOG.info("Applying custom structural execution overrides.")
            # Use deep_merge(base, updates) from source 5 to merge overrides over template_dict
            resolved_dict = deep_merge(template_dict, overrides)
        else:
            resolved_dict = template_dict

        return msgspec.convert(resolved_dict, cls)


class SQLCompiler:
    def __init__(self, dialect: str, user_override: dict[str, Any] | None = None):
        self.dialect = dialect
        try:
            self.dialect_enum = Dialect(dialect.casefold())
        except Exception:
            LOG.warning(
                f"Dialect '{dialect}' not officially registered. Using ANSI defaults."
            )
            self.dialect_enum = Dialect.DEFAULT

        self.template = DialectTemplate.build(self.dialect_enum, user_override)
        LOG.debug(f"Initialized compiler for dialect: '{self.dialect}'")

    def compile(self, operation: SQLOperation | str, **ops_kwargs: Any) -> str:
        """Single entry-point that dispatches compilation to registered decorator handlers."""
        compile_func = COMPILE_FUNCTIONS.get(operation)
        if not compile_func:
            raise SQLCompilationError(
                f"Unsupported SQL operation '{operation}' for dialect '{self.dialect}'"
            )

        LOG.debug(f"Compiling '{operation}' for dialect '{self.dialect}'")
        return compile_func(compiler=self, **ops_kwargs)

    def compile_expr(self, func_name: str, **kwargs: Any) -> str:
        """
        Parses and formats in-flight transformation function templates defined in YAML.

        Example:
            compiler.compile_expression("cast_to_date", val="created_at")
            # Returns: "toDate('created_at')" (quoted per dialect rules)

            compiler.compile_expression("hash_agg", fields=["id", "name"])
            # Returns: "hex(groupBitXor(cityHash64('id', 'name')))"
        """
        expr_template = self.template.function.get(func_name)
        if not expr_template:
            raise SQLCompilationError(
                f"Function template '{func_name}' is not defined for dialect '{self.dialect}'."
            )

        formatted_kwargs = {}
        for key, val in kwargs.items():
            if isinstance(val, list | tuple | set):
                # Quote all items in column lists and join with commas
                quoted_cols = [self.quote_identifier(str(col)) for col in val]
                formatted_kwargs[key] = ", ".join(quoted_cols)
            elif isinstance(val, str):
                # Quote single column or field identifiers
                formatted_kwargs[key] = self.quote_identifier(val)
            else:
                # Retain raw literals (e.g. integers, floats, standard defaults)
                formatted_kwargs[key] = str(val)

        try:
            return expr_template.format(**formatted_kwargs)
        except KeyError as e:
            raise SQLCompilationError(
                f"Missing required parameter {e} for function template '{func_name}' in dialect '{self.dialect}'"
            ) from e

    def _format_operand(self, val: Any) -> str:
        """
        Formats column identifiers or raw expressions for left-hand side operands.
        Quotes standard column names while leaving raw functions/expressions intact.
        """

        def _is_raw_expr(expr: str) -> bool:
            """Determines if a string is a raw SQL expression/function rather than a plain column name."""
            return bool(re.search(r"[\(\)\%\+\-\/\*]|\s", expr))

        if isinstance(val, str) and not _is_raw_expr(val):
            return self.quote_identifier(val)
        return str(val)

    def compile_conditions(
        self, conditions: Predicate | list[Predicate] | None, clause: str = "WHERE"
    ) -> str:
        """
        Generic WHERE clause generator using Python structural pattern matching.
        """
        if not conditions:
            return ""

        if not isinstance(conditions, list):
            conditions = [conditions]

        compiled_fragments = []

        for cond in conditions:
            if not cond:
                continue

            match cond:
                # 1. Raw SQL String
                case str(raw_sql):
                    compiled_fragments.append(raw_sql.strip())

                # 2. Dictionary Conditions (Key-Value map or Raw Expression map)
                case dict(mapping):
                    eq_terms = []
                    for k, v in mapping.items():
                        lhs = self._format_operand(k)
                        rhs = f"'{v}'" if isinstance(v, str) else str(v)
                        eq_terms.append(f"{lhs} = {rhs}")
                    compiled_fragments.append(" AND ".join(eq_terms))

                # 3. Unary / Subquery Existential Predicates: ("NOT EXISTS", "SELECT ...")
                case (str(op), str(subquery)) if op.upper().strip() in (
                    "EXISTS",
                    "NOT EXISTS",
                ):
                    inner_sql = subquery.strip().rstrip(";")
                    compiled_fragments.append(f"{op.upper().strip()} ({inner_sql})")

                # 4. Set / List IN / NOT IN Predicates: ("role", "IN", ["admin", "editor"])
                case (
                    col,
                    str(op),
                    (list() | tuple() | set()) as values,
                ) if op.upper().strip() in ("IN", "NOT IN"):
                    lhs = self._format_operand(col)
                    formatted_vals = ", ".join(
                        [f"'{v}'" if isinstance(v, str) else str(v) for v in values]
                    )
                    compiled_fragments.append(
                        f"{lhs} {op.upper().strip()} ({formatted_vals})"
                    )

                # 5. Subquery IN / NOT IN Predicates: ("gender", "NOT IN", "SELECT ...")
                case (col, str(op), str(subquery)) if op.upper().strip() in (
                    "IN",
                    "NOT IN",
                ):
                    lhs = self._format_operand(col)
                    inner_sql = subquery.strip().rstrip(";")
                    if not (inner_sql.startswith("(") and inner_sql.endswith(")")):
                        inner_sql = f"({inner_sql})"
                    compiled_fragments.append(f"{lhs} {op.upper().strip()} {inner_sql}")

                # 6. Generic Binary Operators: ("age", ">=", 21) or ("status", "=", "ACTIVE")
                case (col, str(op), val):
                    lhs = self._format_operand(col)
                    rhs = f"'{val}'" if isinstance(val, str) else str(val)
                    compiled_fragments.append(f"{lhs} {op.upper().strip()} {rhs}")

                case _:
                    raise SQLCompilationError(
                        f"Unsupported predicate structure in compile_conditions: {cond}"
                    )

        if not compiled_fragments:
            return ""

        joined = " AND ".join([f"({f})" for f in compiled_fragments])
        return f"{clause} {joined}"

    def _build_list_clause(self, keyword: str, items: list[str] | None) -> str:
        """Formats list-based clauses like GROUP BY and ORDER BY."""
        if not items:
            return ""
        formatted = [
            (
                item
                if item.isdigit() or "(" in item or " " in item
                else self.quote_identifier(item)
            )
            for item in items
        ]
        return f"{keyword} {', '.join(formatted)}"

    def _build_joins(self, joins: list[JoinConfig] | None) -> str:
        if not joins:
            return ""
        return "\n".join(
            f"{j.type.value.upper()} JOIN {self.quote_identifier(j.to_table)} ON {j.on}"
            for j in joins
        )

    def quote_identifier(self, identifier: str) -> str:
        """
        Safely quotes system identifiers, preserving paths (e.g. 'analytics.users' -> '"analytics"."users"').
        """
        if not identifier:
            return ""

        if identifier.strip() == "*":
            return "*"

        # Regex check to avoid double-wrapping already quoted fields
        q_open = self.template.identifiers.get("quote_open")
        q_close = self.template.identifiers.get("quote_close")
        parts = identifier.split(".")
        quoted_parts = []
        for part in parts:
            cleaned_part = part.strip()
            if cleaned_part.startswith(q_open) and cleaned_part.endswith(q_close):
                quoted_parts.append(cleaned_part)
            else:
                quoted_parts.append(f"{q_open}{cleaned_part}{q_close}")
        return ".".join(quoted_parts)

    def validate_identifier(self, identifier: str) -> dict[str, Any] | None:
        """
        Parses and validates table identifiers based on target platform rules.
        Handles escaping and quotes dynamically.
        """
        rule = self.template.identifiers

        # Regex split by dots while ignoring dots inside quotes (e.g., "PROD.DB"."SCHEMA"."TABLE")
        parts = re.split(r'\.(?=(?:[^"]*"[^"]*")*[^"]*$)', identifier)

        # parts = next(csv.reader([identifier], delimiter='.'))
        part_count = len(parts)

        if part_count not in rule["allowed_depths"]:
            raise ConfigurationValidationError(
                f"Invalid target object name '{identifier}' for {self.dialect.upper()}. "
                f"Expected format: '{rule['naming_structure']}' ({rule['expected_parts']} parts), "
                f"but found {part_count} parts instead."
            )

        # Base structure matching the database hierarchy order
        keys = ["database", "schema", "table"]

        if self.dialect == "oracle":
            # Oracle strictly expects: SCHEMA.TABLE (pad with None if missing elements)
            padded_parts = ([*parts, None, None])[:2]
            return {
                "database": None,
                "schema": padded_parts[0],
                "table": padded_parts[1],
            }

        # For other databases, slice keys from the right based on how many parts we have
        # e.g., if len is 1 -> ['table'], if len is 2 -> ['schema', 'table']
        active_keys = keys[-len(parts) :] if len(parts) <= 3 else keys

        # Zip them together into a dictionary, defaulting missing keys to None
        components = dict.fromkeys(keys)
        components.update(zip(active_keys, parts[-3:], strict=False))

        return components

    def _map_generic_type(self, raw_type: str) -> str:
        """Maps an generic pipeline data type configuration to its physical target counterpart."""
        tokens = raw_type.strip().split()
        if not tokens:
            raise SQLCompilationError("Encountered empty column type declaration.")

        generic_base = tokens[0].lower()
        native_type = self.template.types.get(generic_base)

        if not native_type:
            LOG.warning(
                f"Unknown generic data type '{generic_base}'. Direct passing through."
            )
            native_type = generic_base.upper()

        # Extract modifying rules (e.g. primary_key, unique, not_null)
        modifiers = []
        full_declaration = " ".join(tokens[1:]).lower()

        if "primary key" in full_declaration and self.dialect in [
            "postgres",
            "snowflake",
        ]:
            modifiers.append("PRIMARY KEY")
        if "not null" in full_declaration or "not_null" in full_declaration:
            modifiers.append("NOT NULL")
        elif "unique" in full_declaration:
            modifiers.append("UNIQUE")

        return f"{native_type} {' '.join(modifiers)}".strip()

    # def validate_replication(self) -> list[str]:
    #     """Validates all configured streams in the loaded YAML definition."""
    #     errors = []
    #     streams = self.config.get("streams", {})

    #     logger_target = self.target_type.upper()
    #     print(
    #         f"Executing Stream Validation Suite for Target Engine: {logger_target}\n"
    #         + "-" * 50
    #     )

    #     for stream_name, stream_config in streams.items():
    #         target_object = stream_config.get("object")
    #         if not target_object:
    #             continue

    #         try:
    #             parsed_metadata = self.validate_identifier(target_object)
    #             print(
    #                 f"✓ '{stream_name}' -> '{target_object}' is VALID for {logger_target}."
    #             )
    #             print(f"   Parsed Hierarchy: {parsed_metadata}")
    #         except ConfigurationValidationError as e:
    #             errors.append(str(e))
    #             print(f"✗ VALIDATION FAILURE: {e}")

    #     return errors

    def compile_select_block(self, block: SelectQueryContext) -> str:
        """
        Compiles a single SelectQueryContext into a valid SQL SELECT statement.

        If `sql` is provided alongside other clause fields (e.g. select, where, group_by),
        the `sql` block is wrapped as a FROM subquery.
        """
        # 0. Validation: Ensure a source (from_table or sql) is provided
        if not block.from_table and not block.sql:
            block_identifier = (
                f"CTE '{block.name}'" if block.name else "Main query block"
            )
            raise SQLCompilationError(
                f"{block_identifier} must specify either 'from_table' or 'sql'."
            )

        # 1. Pure raw SQL override (no additional clauses or from_table specified)
        if block.sql and not block.has_clauses and not block.from_table:
            return block.sql.strip().rstrip(";")

        # 2. SELECT & FROM definitions
        cols = (
            ", ".join(
                c if "(" in c or " " in c else self.quote_identifier(c)
                for c in block.select
            )
            if block.select
            else "*"
        )

        if block.sql:
            alias = (
                self.quote_identifier(block.from_table)
                if block.from_table
                else "_subquery"
            )
            from_clause = f"FROM ({block.sql.strip().rstrip(';')}) AS {alias}"
        elif block.from_table:
            from_clause = f"FROM {self.quote_identifier(block.from_table)}"
        else:
            from_clause = ""

        # 3. Declarative rendering pipeline
        pipeline = [
            f"SELECT {cols}",
            from_clause,
            self._build_joins(block.joins),
            self.compile_conditions(block.where, clause="WHERE"),
            self._build_list_clause("GROUP BY", block.group_by),
            self.compile_conditions(block.having, clause="HAVING"),
            self.compile_conditions(block.qualify, clause="QUALIFY"),
            self._build_list_clause("ORDER BY", block.order_by),
            f"LIMIT {block.limit}" if block.limit is not None else "",
        ]

        # Join non-empty string fragments
        return "\n".join(part for part in pipeline if part)

    def compile_context(self, ctx: SQLContext) -> str:
        """Compiles a complete SQLContext (including CTEs and main query) into a final SQL string."""
        # 1. Top-level raw SQL override
        if ctx.sql:
            return ctx.sql.strip().rstrip(";")

        # 2. Compile CTEs
        cte_sql_parts = []
        for cte in ctx.ctes:
            if not cte.name:
                raise SQLCompilationError(
                    "Every CTE block in 'ctes' must specify a 'name'."
                )
            block_sql = self.compile_select_block(cte)
            cte_sql_parts.append(
                f"{self.quote_identifier(cte.name)} AS (\n{block_sql}\n)"
            )

        with_clause = ""
        if cte_sql_parts:
            with_clause = "WITH " + ",\n".join(cte_sql_parts) + "\n"

        # 3. Compile Main Query Block
        if ctx.main:
            main_sql = self.compile_select_block(ctx.main)
        elif ctx.ctes and (last_cte_name := ctx.ctes[-1].name):
            # Default main query to selecting everything from the last CTE
            main_sql = f"SELECT * FROM {self.quote_identifier(last_cte_name)}"
        else:
            raise SQLCompilationError(
                "SQLContext must contain either 'ctes', 'main', or 'sql'."
            )

        return f"{with_clause}{main_sql}"
