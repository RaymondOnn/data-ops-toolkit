import logging
import re
from dataclasses import dataclass, field
from typing import Any

import yaml

LOG = logging.getLogger(__name__)


class SQLCompilationError(Exception):
    """Raised when compilation fails due to mismatched syntax, missing keys, or type errors."""

    pass


class ConfigurationValidationError(Exception): ...


@dataclass
class DialectConfig:
    name: str
    quote_char: str = '"'
    core: dict[str, str] = field(default_factory=dict)
    types: dict[str, str] = field(default_factory=dict)


BASE_DIALECT_TEMPLATE: dict[str, Any] = {
    "quote_char": '"',
    "core": {
        "drop_table": "DROP TABLE IF EXISTS {table};",
        "create_table": "CREATE TABLE IF NOT EXISTS {table} ({col_types});",
        "truncate": "TRUNCATE TABLE {table};",
        "insert": "INSERT INTO {table} ({fields}) VALUES ({values});",
    },
    "types": {
        "integer": "INT",
        "string": "VARCHAR(255)",
        "boolean": "BOOLEAN",
        "datetime": "TIMESTAMP",
        "decimal": "DECIMAL(38,9)",
        "json": "TEXT",
    },
}

DIALECT_OVERLAYS = {
    "postgres": {
        "quote_char": '"',
        "core": {
            "upsert": "INSERT INTO {table} ({fields}) VALUES ({values}) ON CONFLICT ({pk}) DO UPDATE SET {set_values};"
        },
        "types": {
            "integer": "BIGINT",
            "json": "JSONB",
            "datetime": "TIMESTAMP WITH TIME ZONE",
        },
    },
    "snowflake": {
        "quote_char": '"',
        "core": {
            "upsert": """MERGE INTO {table} AS tgt
USING staging_{table} AS src
ON tgt.{pk} = src.{pk}
WHEN MATCHED THEN
  UPDATE SET {set_values}
WHEN NOT MATCHED THEN
  INSERT ({fields}) VALUES ({src_fields});"""
        },
        "types": {
            "integer": "NUMBER(38,0)",
            "string": "VARCHAR(16777216)",
            "datetime": "TIMESTAMP_TZ",
            "json": "VARIANT",
        },
    },
    "clickhouse": {
        "quote_char": "`",
        "core": {
            "create_table": "CREATE TABLE IF NOT EXISTS {table} ({col_types}) ENGINE = MergeTree() ORDER BY tuple();",
        },
        "types": {
            "integer": "Nullable(Int64)",
            "string": "Nullable(String)",
            "datetime": "Nullable(DateTime64(6, 'UTC'))",
            "json": "Nullable(String)",
        },
    },
}


class SlingSQLCompiler:
    def __init__(self, dialect: str, user_override: dict[str, Any] | None = None):
        self.dialect_name = dialect.lower()
        self.dialect = self._build_dialect(self.dialect_name, user_override)
        LOG.info(f"Initialized compiler for dialect: '{self.dialect_name}'")

    def _build_dialect(
        self, dialect: str, user_override: dict[str, Any] | None
    ) -> DialectConfig:
        """Loads, merges, and overrides configuration layers down into a final DialectConfig."""
        # 1. Base initialization
        merged_core = BASE_DIALECT_TEMPLATE["core"].copy()
        merged_types = BASE_DIALECT_TEMPLATE["types"].copy()
        quote_char = BASE_DIALECT_TEMPLATE["quote_char"]

        # 2. Layer Dialect Overrides
        overlay = DIALECT_OVERLAYS.get(dialect)
        if not overlay:
            LOG.warning(
                f"Dialect '{dialect}' not officially registered. Using ANSI defaults."
            )
            overlay = {}

        quote_char = overlay.get("quote_char", quote_char)
        merged_core.update(overlay.get("core", {}))
        merged_types.update(overlay.get("types", {}))

        # 3. Layer Custom User-Supplied Run overrides
        if user_override:
            LOG.info("Applying custom structural execution overrides.")
            quote_char = user_override.get("quote_char", quote_char)
            merged_core.update(user_override.get("core", {}))
            merged_types.update(user_override.get("types", {}))

        return DialectConfig(
            name=dialect, quote_char=quote_char, core=merged_core, types=merged_types
        )

    def quote_identifier(self, identifier: str) -> str:
        """
        Safely quotes system identifiers, preserving paths (e.g. 'analytics.users' -> '"analytics"."users"').
        """
        if not identifier:
            return ""

        # Regex check to avoid double-wrapping already quoted fields
        q = self.dialect.quote_char
        parts = identifier.split(".")
        quoted_parts = []
        for part in parts:
            cleaned_part = part.strip()
            if cleaned_part.startswith(q) and cleaned_part.endswith(q):
                quoted_parts.append(cleaned_part)
            else:
                quoted_parts.append(f"{q}{cleaned_part}{q}")
        return ".".join(quoted_parts)

    def _map_generic_type(self, raw_type: str) -> str:
        """Maps an generic pipeline data type configuration to its physical target counterpart."""
        tokens = raw_type.strip().split()
        if not tokens:
            raise SQLCompilationError("Encountered empty column type declaration.")

        generic_base = tokens[0].lower()
        native_type = self.dialect.types.get(generic_base)

        if not native_type:
            LOG.warning(
                f"Unknown generic data type '{generic_base}'. Direct passing through."
            )
            native_type = generic_base.upper()

        # Extract modifying rules (e.g. primary_key, unique, not_null)
        modifiers = []
        full_declaration = " ".join(tokens[1:]).lower()

        if "primary key" in full_declaration and self.dialect_name in [
            "postgres",
            "snowflake",
        ]:
            modifiers.append("PRIMARY KEY")
        if "not null" in full_declaration or "not_null" in full_declaration:
            modifiers.append("NOT NULL")
        elif "unique" in full_declaration:
            modifiers.append("UNIQUE")

        return f"{native_type} {' '.join(modifiers)}".strip()

    def compile_create_table(
        self, table_name: str, schema_columns: dict[str, str]
    ) -> str:
        """Constructs a clean DDL CREATE TABLE statement based on schema definitions."""
        if not schema_columns:
            raise SQLCompilationError(
                "Cannot compile CREATE TABLE without column fields."
            )

        quoted_table = self.quote_identifier(table_name)
        col_declarations = []

        for column, type_expr in schema_columns.items():
            quoted_col = self.quote_identifier(column)
            native_type_declaration = self._map_generic_type(type_expr)
            col_declarations.append(f"{quoted_col} {native_type_declaration}")

        col_types_str = ", ".join(col_declarations)

        template = self.dialect.core.get("create_table")
        if not template:
            raise SQLCompilationError(
                f"No CREATE TABLE template configured for dialect '{self.dialect_name}'"
            )

        return template.format(table=quoted_table, col_types=col_types_str)

    def compile_upsert(
        self, table_name: str, schema_columns: dict[str, str], primary_key: str
    ) -> str:
        """Constructs highly-optimized target upsert routines based on dialect syntax rules."""
        if primary_key not in schema_columns:
            raise SQLCompilationError(
                f"Specified primary key '{primary_key}' must exist in columns schema map."
            )

        quoted_table = self.quote_identifier(table_name)
        quoted_pk = self.quote_identifier(primary_key)

        fields = [self.quote_identifier(col) for col in schema_columns]
        fields_str = ", ".join(fields)

        # Build parameterized placeholder bindings (e.g., :column_name)
        value_bindings = [f":{col}" for col in schema_columns]
        values_str = ", ".join(value_bindings)

        # Build set allocations
        set_statements = []
        for col in schema_columns:
            if col == primary_key:
                continue
            quoted_col = self.quote_identifier(col)
            if self.dialect_name == "snowflake":
                set_statements.append(f"tgt.{quoted_col} = src.{quoted_col}")
            else:
                set_statements.append(f"{quoted_col} = EXCLUDED.{quoted_col}")

        set_values_str = ", ".join(set_statements)
        src_fields_str = ", ".join([f"src.{f}" for f in fields])

        template = self.dialect.core.get("upsert")
        if not template:
            raise SQLCompilationError(
                f"Upsert routine is not supported dynamically on '{self.dialect_name}' driver."
            )

        # Clean formatting interpolation
        return template.format(
            table=quoted_table,
            fields=fields_str,
            values=values_str,
            src_fields=src_fields_str,
            pk=quoted_pk,
            set_values=set_values_str,
        )


DB_HIERARCHY_RULES = {
    "oracle": {
        "expected_parts": 2,
        "naming_structure": "SCHEMA.TABLE",
        "allowed_depths": [2],  # Oracle ignores "database" in qualified paths
    },
    "mysql": {
        "expected_parts": 2,
        "naming_structure": "DATABASE.TABLE",  # MySQL uses database and schema interchangeably
        "allowed_depths": [2],
    },
    "postgres": {
        "expected_parts": 2,
        "naming_structure": "[SCHEMA.]TABLE",  # Can resolve default database
        "allowed_depths": [1, 2, 3],
    },
    "snowflake": {
        "expected_parts": 3,
        "naming_structure": "[DATABASE.][SCHEMA.]TABLE",
        "allowed_depths": [1, 2, 3],
    },
}


class ConfigValidator:
    def __init__(self, raw_yaml_config: str):
        self.config = yaml.safe_load(raw_yaml_config)
        self.target_type = self._resolve_target_type()

    def _resolve_target_type(self) -> str:
        """Looks up target connection rules to discover the system type (e.g., oracle)."""
        connections = self.config.get("connections", {})
        # Let's assume we target the first connection for replication target validation
        for _, conn_details in connections.items():
            if "type" in conn_details:
                return conn_details["type"].lower()
        return "postgres"  # Standard default fallback

    def validate_identifier(self, identifier: str) -> dict[str, str]:
        """
        Parses and validates table identifiers based on target platform rules.
        Handles escaping and quotes dynamically.
        """
        rule = DB_HIERARCHY_RULES.get(self.target_type)
        if not rule:
            # Fallback to standard ANSI defaults if database driver isn't registered
            rule = {
                "expected_parts": 2,
                "allowed_depths": [1, 2, 3],
                "naming_structure": "SCHEMA.TABLE",
            }

        # Regex split by dots while ignoring dots inside quotes (e.g., "PROD.DB"."SCHEMA"."TABLE")
        parts = re.split(r'\.(?=(?:[^"]*"[^"]*")*[^"]*$)', identifier)
        part_count = len(parts)

        if part_count not in rule["allowed_depths"]:
            raise ConfigurationValidationError(
                f"Invalid target object name '{identifier}' for {self.target_type.upper()}. "
                f"Expected format: '{rule['naming_structure']}' ({rule['expected_parts']} parts), "
                f"but found {part_count} parts instead."
            )

        # Map components cleanly based on found depth
        components = {"database": None, "schema": None, "table": None}

        if self.target_type == "oracle":
            # Oracle strictly expects: SCHEMA.TABLE
            components["schema"] = parts[0]
            components["table"] = parts[1]
        elif part_count == 3:
            components["database"] = parts[0]
            components["schema"] = parts[1]
            components["table"] = parts[2]
        elif part_count == 2:
            components["schema"] = parts[0]
            components["table"] = parts[1]
        else:
            components["table"] = parts[0]

        return components

    def validate_replication(self) -> list[str]:
        """Validates all configured streams in the loaded YAML definition."""
        errors = []
        streams = self.config.get("streams", {})

        logger_target = self.target_type.upper()
        print(
            f"Executing Stream Validation Suite for Target Engine: {logger_target}\n"
            + "-" * 50
        )

        for stream_name, stream_config in streams.items():
            target_object = stream_config.get("object")
            if not target_object:
                continue

            try:
                parsed_metadata = self.validate_identifier(target_object)
                print(
                    f"✓ '{stream_name}' -> '{target_object}' is VALID for {logger_target}."
                )
                print(f"   Parsed Hierarchy: {parsed_metadata}")
            except ConfigurationValidationError as e:
                errors.append(str(e))
                print(f"✗ VALIDATION FAILURE: {e}")

        return errors


def validate_fully_qualified_table(object_name: str, expected_parts: int = 3) -> dict:
    """
    Validates if a table name is fully qualified.
    expected_parts = 2 for 'schema.table'
    expected_parts = 3 for 'database.schema.table'
    """
    # Split by dot, ignoring escaped dots (e.g., "my.database"."my.schema"."table")
    parts = re.split(r'\.(?=(?:[^"]*"[^"]*")*[^"]*$)', object_name)

    if len(parts) < expected_parts:
        raise ValueError(
            f"Table name '{object_name}' is not fully qualified. "
            f"Expected {expected_parts} parts, but got {len(parts)}."
        )

    return {
        "database": parts[0] if len(parts) == 3 else None,
        "schema": parts[1] if len(parts) == 3 else parts[0],
        "table": parts[-1],
    }


# Example Usage:
try:
    # Will pass for 3-part target
    parsed = validate_fully_qualified_table(
        "dw_prod.analytics.orders", expected_parts=3
    )
    print("Valid:", parsed)

    # Will raise ValueError
    validate_fully_qualified_table("orders", expected_parts=2)
except ValueError as e:
    print("Validation Failed:", e)
