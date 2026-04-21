import logging
import time
import uuid
from collections.abc import Generator, Sequence
from pathlib import Path
from typing import Any

import polars as pl
from clickhouse_connect.driver.client import Client

from ..pool.base import ConnectionPool
from ..pool.queue import QueueConnectionPool
from .base import DBClient

LOG = logging.getLogger(__name__)


class ClickhouseClient(DBClient):
    def __init__(self, **config: Any):
        super().__init__(**config)

    @property
    def type(self) -> str:
        return "clickhouse"

    def _init_pool(self) -> ConnectionPool:
        pool_size = self.config.get("pool_size", 0)
        if pool_size > 1:
            LOG.info("Initializing ClickHouse Queue Pool", extra={"size": pool_size})
            return QueueConnectionPool(connector=self.connect, size=pool_size)
        return super()._init_pool()

    def connect(self) -> Client:
        # Import inside so that Ray workers can import
        import clickhouse_connect
        from libs.clients.base import ClientCantConnect

        try:
            conn = clickhouse_connect.get_client(
                host=str(self.config.get("host", "localhost")),
                port=int(self.config.get("port", 8123)),
                username=str(self.config.get("user")),
                password=str(self.config.get("password")),
                # timeout=self.config.get("timeout", 30),  # Connection timeout in seconds
                # send_receive_timeout=self.config.get("send_receive_timeout", 300) # Data transfer timeout in seconds
            )
            self._ping(conn)
            return conn
        except Exception as e:
            raise ClientCantConnect("Failed to connect to ClickHouse") from e

    def _ping(self, conn: Client) -> None:
        conn.ping()

    def get_load_strategy(
        self,
        table_name: str,
        num_workers: int = 5,
        filter_sql: str | None = None,
    ) -> set[str]:
        filter_sql = filter_sql.replace("WHERE", "") if filter_sql else ""
        return {
            f"""
            SELECT * FROM {table_name} 
            WHERE {filter_sql} 
            AND cityHash64(*) % {num_workers} = {i}
            """
            for i in range(num_workers)
        }

    def copy_from_file(
        self,
        table: str,
        source_dir: str,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> None:
        audit_values = audit_values or {}
        files = list(Path(source_dir).glob(f"*.{file_ext}"))

        if not files:
            LOG.warning(
                "No files found for staging",
                extra={"source_dir": source_dir, "file_ext": file_ext},
            )
            return

        schema_info = self.sql(f"DESCRIBE TABLE {table}")
        all_columns = [
            row[0].decode("utf-8") if isinstance(row[0], bytes) else str(row[0])
            for row in schema_info
        ]
        # Filter columns to match the staging table (excluding audit columns)
        column_names = [c for c in all_columns if c not in audit_values]

        # 1. Create a Temporary Table with the same structure as the Parquet
        # 'AS target_table' copies the schema; 'EXCEPT' omits the audit columns
        unique_id = str(uuid.uuid4())[:8]
        tmp_table = f"tmp_stage_{int(time.time())}_{unique_id}"
        except_clause = (
            f"EXCEPT ({', '.join(audit_values.keys())})" if audit_values else ""
        )

        with self.get_connection() as conn:
            conn.command(
                f"""
                CREATE TEMPORARY TABLE {tmp_table} 
                ENGINE = MergeTree()
                ORDER BY tuple()
                AS 
                SELECT * {except_clause}
                FROM {table}
                LIMIT 0 
            """
            )

            # 2. Bulk load the files into the Temp Table
            for file in files:
                with file.open("rb") as f:
                    # Streams the binary data directly
                    conn.raw_insert(
                        table=tmp_table,
                        insert_block=f,
                        column_names=column_names,
                        fmt=file.suffix.lstrip(".").title(),
                    )

            # 3. Move to the Target Table with Audit Constants
            select_clause = "*"
            if audit_values:
                audit_sql = ", ".join(
                    [f"'{v}' AS {k}" for k, v in audit_values.items()]
                )
                select_clause = f"*, {audit_sql}"

            LOG.debug(
                "Promoting from temp stage to target",
                extra={"table": table, "select": select_clause},
            )
            conn.command(
                f"""
                INSERT INTO {table} 
                SELECT {select_clause}
                FROM {tmp_table}
            """
            )
            LOG.info(
                "Successfully staged files to ClickHouse",
                extra={"table": table, "count": len(files)},
            )

    def sql(self, query: str) -> list[Sequence[Any]]:
        # Returns a list of tuples by default
        LOG.debug("Executing SQL query", extra={"query": query})
        with self.get_connection() as conn:
            result = conn.query(query)
            return list(result.result_rows)

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        # clickhouse-connect supports native DataFrame streaming
        LOG.debug("Executing SQL query", extra={"query": query})
        with self.get_connection() as conn:
            result = conn.query_df_stream(query, settings={"max_block_size": 100_000})
            with result:
                for pandas_df in result:
                    yield pl.from_pandas(pandas_df)

    def get_schema(self, fq_table: str) -> pl.DataFrame:
        # ClickHouse has a system.columns table we can query for schema info
        database, table_name = fq_table.split(".")
        query = f"""
            SELECT 
                name AS column_name, 
                type AS data_type, 
                is_in_primary_key,
                -- ClickHouse doesn't use precision/scale for all types, 
                -- but it's available for Decimal types
                numeric_precision,
                numeric_scale
            FROM system.columns
            WHERE database = '{database}' 
            AND table = '{table_name}'
            ORDER BY position;
        """
        return pl.concat(self.fetch_df(query), how="vertical")

    def exists(self, fq_table: str) -> bool:
        """Uses ClickHouse EXISTS TABLE command."""
        database, table = (
            fq_table.split(".") if "." in fq_table else ("default", fq_table)
        )
        # EXISTS TABLE returns 1 or 0
        res = self.sql(f"EXISTS TABLE {database}.{table}")
        return bool(res[0][0]) if res else False
