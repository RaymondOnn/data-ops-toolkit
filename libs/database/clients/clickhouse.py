import logging
import re
import time
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pyarrow as pa
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from libs.database.pool.base import ConnectionPool
from libs.database.pool.queue import QueueConnectionPool
from libs.utils.exceptions import AuthFailure, HostUnreachable

from .base import DBClient
from .factory import DatabaseFactory

LOG = logging.getLogger(__name__)


def get_error_code(exception):
    """
    Extracts the numeric ClickHouse error code from an exception message.

    Args:
        exception: The exception object to parse.

    Returns:
        int | None: The parsed error code if found, otherwise None.
    """
    match = re.search(r"Code:\s*(\d+)", str(exception))
    return int(match.group(1)) if match else None


@DatabaseFactory.register
class ClickhouseClient(DBClient):
    """
    High-performance client for ClickHouse using clickhouse-connect.

    Supports connection pooling, streaming Polars DataFrames, and bulk loading
    via temporary staging tables.
    """

    type: str = "clickhouse"

    def __init__(self, **config: Any):
        """Initializes the ClickhouseClient with provided config."""
        super().__init__(**config)

    def _init_pool(self) -> ConnectionPool:
        """
        Initializes the connection pool. Uses Queue pool if size > 1.

        Returns:
            ConnectionPool: The initialized pool instance.
        """
        pool_size = self.config.get("pool_size", 0)
        if pool_size > 1:
            LOG.info("Initializing Pool", extra={"size": pool_size})
            return QueueConnectionPool(connector=self.connect, size=pool_size)
        return super()._init_pool()

    def connect(self) -> Client:
        """
        Establishes a connection to the ClickHouse server.

        Returns:
            Client: An active clickhouse-connect client.

        Raises:
            AuthFailure, HostUnreachable, ClientCantConnect: On connection errors.
        """
        # Import inside so that Ray workers can import
        import clickhouse_connect

        from libs.clients.base import ClientCantConnect

        try:
            conn = clickhouse_connect.get_client(
                host=str(self.config.get("host", "localhost")),
                port=int(self.config.get("port", 8123)),
                username=str(self.config.get("user")),
                password=str(self.config.get("password")),
                # # Connection timeout in seconds
                # timeout=self.config.get("timeout", 30),
                # # Data transfer timeout in seconds
                # send_receive_timeout=self.config.get("send_receive_timeout", 300)
            )
            self._ping(conn)
            return conn
        except (DatabaseError, OperationalError) as e:
            code = get_error_code(e)

            if code in (516, 192, 193):
                raise AuthFailure() from e

            if code in (209, 210) or "Connection refused" in str(e):
                raise HostUnreachable() from e
            raise ClientCantConnect("Failed to connect to ClickHouse") from e
        except Exception as e:
            raise ClientCantConnect("Failed to connect to ClickHouse") from e

    def _ping(self, conn: Client) -> None:
        """Verifies connection health."""
        conn.ping()

    def command(self, sql: str, params: Any = None) -> None:
        with self.get_connection() as conn:
            conn.command(sql, parameters=params)

    def query(
        self, sql: str, params: Any = None
    ) -> Generator[pa.RecordBatch, None, None]:
        """Streams query results as Polars DataFrames via Apache Arrow."""
        LOG.debug("Executing streaming Arrow query", extra={"query": sql})
        parameters = params if isinstance(params, dict) else None

        with self.get_connection() as conn:
            # Yields native Apache Arrow streams directly from ClickHouse
            result = conn.query_arrow_stream(sql, parameters=parameters)
            with result:
                yield from result

    def copy(
        self,
        table: str,
        source_dir: str,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> None:
        """
        Multi-stage staging bulk load into ClickHouse:
        1. Creates a temporary table with the schema matching the target.
        2. Streams binary Parquet files into the staging table via raw_insert().
        3. Promotes staged data into target while injecting audit constants.
        """
        audit_values = audit_values or {}
        files = list(Path(source_dir).glob(f"*.{file_ext}"))

        if not files:
            LOG.warning(f"No {file_ext} files found in {source_dir}")
            return

        # Fetch columns from system layout to match target schema
        with self.get_connection() as conn:
            schema_info = conn.query(f"DESCRIBE TABLE {table}").result_rows
            all_columns = [str(row[0]) for row in schema_info]
            column_names = [c for c in all_columns if c not in audit_values]

            # 1. Create Temporary Table
            unique_id = str(uuid.uuid4())[:8]
            tmp_table = f"tmp_stage_{int(time.time())}_{unique_id}"
            except_clause = (
                f"EXCEPT ({', '.join(audit_values.keys())})" if audit_values else ""
            )

            conn.command(f"""
                CREATE TEMPORARY TABLE {tmp_table}
                ENGINE = MergeTree() ORDER BY tuple()
                AS SELECT * {except_clause} FROM {table} LIMIT 0
            """)

            # 2. Bulk Insert Parquet Streams
            for file_path in files:
                with file_path.open("rb") as f:
                    conn.raw_insert(
                        table=tmp_table,
                        insert_block=f,
                        column_names=column_names,
                        fmt=file_ext.capitalize(),
                    )

            # 3. Promote to Target Table with Audit Injectors
            audit_sql = ""
            if audit_values:
                audit_sql = ", " + ", ".join(
                    [f"'{v}' AS {k}" for k, v in audit_values.items()]
                )

            conn.command(f"""
                INSERT INTO {table}
                SELECT * {audit_sql} FROM {tmp_table}
            """)

            LOG.info(
                f"Successfully loaded {len(files)} file(s) into ClickHouse table '{table}'"
            )

    # def copy_from_file(
    #     self,
    #     table: str,
    #     source_dir: str,
    #     file_ext: str = "parquet",
    #     audit_values: dict[str, Any] | None = None,
    # ) -> None:
    #     """
    #     Performs a multi-stage bulk load from local files into ClickHouse.

    #     Args:
    #         table: Destination table name.
    #         source_dir: Directory containing files to load.
    #         file_ext: Format of the source files.
    #         audit_values: Constants to inject during promotion to target table.
    #     """
    #     audit_values = audit_values or {}
    #     files = list(Path(source_dir).glob(f"*.{file_ext}"))

    #     if not files:
    #         LOG.warning(
    #             "No files found for staging",
    #             extra={"source_dir": source_dir, "file_ext": file_ext},
    #         )
    #         return

    #     schema_info = self.sql(f"DESCRIBE TABLE {table}")
    #     all_columns = [
    #         row[0].decode("utf-8") if isinstance(row[0], bytes) else str(row[0])
    #         for row in schema_info
    #     ]
    #     # Filter columns to match the staging table (excluding audit columns)
    #     column_names = [c for c in all_columns if c not in audit_values]

    #     # 1. Create a Temporary Table with the same structure as the Parquet
    #     # 'AS target_location' copies the schema; 'EXCEPT' omits the audit columns
    #     unique_id = str(uuid.uuid4())[:8]
    #     tmp_table = f"tmp_stage_{int(time.time())}_{unique_id}"
    #     except_clause = (
    #         f"EXCEPT ({', '.join(audit_values.keys())})" if audit_values else ""
    #     )

    #     with self.get_connection() as conn:
    #         conn.command(f"""
    #             CREATE TEMPORARY TABLE {tmp_table}
    #             ENGINE = MergeTree()
    #             ORDER BY tuple()
    #             AS
    #             SELECT * {except_clause}
    #             FROM {table}
    #             LIMIT 0
    #         """)

    #         # 2. Bulk load the files into the Temp Table
    #         for file in files:
    #             with file.open("rb") as f:
    #                 # Streams the binary data directly
    #                 conn.raw_insert(
    #                     table=tmp_table,
    #                     insert_block=f,
    #                     column_names=column_names,
    #                     fmt=file.suffix.lstrip(".").title(),
    #                 )

    #         # 3. Move to the Target Table with Audit Constants
    #         select_clause = "*"
    #         if audit_values:
    #             audit_sql = ", ".join(
    #                 [f"'{v}' AS {k}" for k, v in audit_values.items()]
    #             )
    #             select_clause = f"*, {audit_sql}"

    #         LOG.debug(
    #             "Promoting from temp stage to target",
    #             extra={"table": table, "select": select_clause},
    #         )
    #         conn.command(f"""
    #             INSERT INTO {table}
    #             SELECT {select_clause}
    #             FROM {tmp_table}
    #         """)
    #         LOG.info(
    #             "Successfully staged files to ClickHouse",
    #             extra={"table": table, "count": len(files)},
    #         )

    # def sql(self, query: str) -> list[Sequence[Any]]:
    #     """
    #     Executes a query and returns results as raw tuples.

    #     Args:
    #         query: The SQL query string.

    #     Returns:
    #         list[Sequence[Any]]: Result rows.
    #     """
    #     LOG.debug("Executing SQL query", extra={"query": query})
    #     with self.get_connection() as conn:
    #         result = conn.query(query)
    #         return list(result.result_rows)

    # def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
    #     """
    #     Executes a query and yields Polars DataFrames using native streaming.

    #     Args:
    #         query: The SQL query string.

    #     Yields:
    #         pl.DataFrame: A batch of query results.
    #     """
    #     LOG.debug("Executing SQL query", extra={"query": query})
    #     with self.get_connection() as conn:
    #         result = conn.query_df_stream(query, settings={"max_block_size": 100_000})
    #         with result:
    #             for pandas_df in result:
    #                 yield pl.from_pandas(pandas_df)

    # def get_schema(self, fq_table: str) -> pl.DataFrame:
    #     """
    #     Retrieves physical schema information from system.columns.

    #     Args:
    #         fq_table: Fully qualified table name.

    #     Returns:
    #         pl.DataFrame: Schema metadata report.
    #     """
    #     database, table_name = fq_table.split(".")
    #     query = f"""
    #         SELECT
    #             name AS column_name,
    #             type AS data_type,
    #             is_in_primary_key,
    #             -- ClickHouse doesn't use precision/scale for all types,
    #             -- but it's available for Decimal types
    #             numeric_precision,
    #             numeric_scale
    #         FROM system.columns
    #         WHERE database = '{database}'
    #         AND table = '{table_name}'
    #         ORDER BY position
    #     """
    #     return pl.concat(self.fetch_df(query), how="vertical")

    # def exists(self, fq_table: str) -> bool:
    #     """
    #     Checks for table existence using ClickHouse native command.

    #     Args:
    #         fq_table: Table name to check.

    #     Returns:
    #         bool: True if it exists, False otherwise.
    #     """
    #     database, table = (
    #         fq_table.split(".") if "." in fq_table else ("default", fq_table)
    #     )
    #     # EXISTS TABLE returns 1 or 0
    #     res = self.sql(f"EXISTS TABLE {database}.{table}")
    #     return bool(res[0][0]) if res else False
