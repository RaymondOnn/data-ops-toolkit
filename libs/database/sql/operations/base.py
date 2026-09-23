from enum import StrEnum
from typing import TYPE_CHECKING, Any

from libs.database.sql.exceptions import SQLCompilationError
from libs.metaclasses.draft import FunctionRegistry

if TYPE_CHECKING:
    from libs.database.sql.compile import SQLCompiler


class SQLOperationType(StrEnum):
    # core
    CREATE_TABLE = "create_table"
    CREATE_SCHEMA = "create_schema"
    TRUNCATE = "truncate"
    INSERT = "insert"
    UPDATE = "update"

    DROP_TABLE = "drop_table"
    DROP_VIEW = "drop_view"
    DROP_SCHEMA = "drop_schema"
    DESCRIBE_TABLE = "describe_table"
    LIKE_TABLE = "like_table"
    CLONE_TABLE = "clone_table"

    ADD_COLUMN = "add_column"

    # merge
    UPSERT = "upsert"
    MERGE_INSERT = "merge_insert"
    MERGE_DELETE = "merge_delete"
    MERGE_UPSERT = "merge_upsert"
    MERGE_UPDATE = "merge_update"

    # metadata
    EXIST = "table_exists"
    COLUMNS = "columns"

    # analysis
    SELECT = "select"
    COUNT = "count"
    MINUS = "minus"


class SQLOperation(
    FunctionRegistry,
    registry_name="SQLOperationRegistry",
    auto_key=False,
    package_paths="libs.database.sql.operations",
):
    @classmethod
    def compile(
        cls,
        operation_type: SQLOperationType | str,
        compiler: "SQLCompiler",
        *args: Any,
        **kwargs: Any,
    ) -> str:
        """Compiles a SQL query by invoking the registered function for the given operation.

        Args:
            operation_type: Target operation key or enum value.
            compiler: SQLCompiler instance containing dialect and configuration.
            *args: Positional arguments for the compilation function.
            **kwargs: Keyword arguments for the compilation function.

        Returns:
            The compiled SQL string.

        Raises:
            SQLCompilationError: If operation_type is not registered or template is missing.
        """
        key = str(operation_type)
        try:
            return cls.invoke(key, compiler, *args, **kwargs)
        except KeyError as e:
            raise SQLCompilationError(
                f"Unsupported SQL operation: '{key}' for dialect '{compiler.dialect}'."
            ) from e
