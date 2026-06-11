import logging
from collections.abc import Generator, Sequence
from typing import Any

import polars as pl
import pyarrow.dataset as ds
from libs.database.clients.base import DBClient
from oracledb import Connection

LOG = logging.getLogger(__name__)

# Note: Running on Thin mode; no instant client required


class OracleClient(DBClient):
    def __init__(self, **config: Any) -> None:
        super().__init__(**config)

    @property
    def type(self) -> str:
        return "oracle"

    def connect(self) -> Connection:
        import oracledb
        from libs.clients.base import ClientCantConnect

        try:
            conn = oracledb.connect(
                user=self.config["user"],
                password=self.config["password"],
                dsn="{}:{}/{}".format(
                    self.config["host"],
                    self.config.get("port", 1521),
                    self.config["service"],
                ),
            )
            self._ping(conn)
            return conn
        except Exception as e:
            raise ClientCantConnect("Failed to connect to Oracle") from e

    def _ping(self, conn: Connection) -> None:
        conn.ping()

    def partition_load(
        self,
        table_name: str,
        num_workers: int = 10,
        filter_condition: str | None = None,
    ) -> set[str]:
        """
        Uses ORA_HASH to create N virtual partitions without needing a PK.
        """
        filter_condition = (
            filter_condition.replace("WHERE", "") if filter_condition else ""
        )
        queries = []
        for i in range(num_workers):
            # ORA_HASH(rowid, N) creates N buckets based on physical location
            sql = f"""
                SELECT * FROM {table_name}
                WHERE {filter_condition}
                AND ORA_HASH(rowid, {num_workers - 1}) = {i}
            """
            queries.append(sql)
        return set(queries)

    def copy_from_file(
        self,
        table: str,
        source_dir: str,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> None:
        audit_values = audit_values or {}

        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table.upper()} WHERE 1=0")
            db_cols = [col[0].lower() for col in cur.description]

            parquet_cols = [c for c in db_cols if c not in audit_values]
            placeholders = ", ".join([f":{i + 1}" for i in range(len(parquet_cols))])
            audit_fragments = ", ".join(
                [
                    f"'{v}'" if isinstance(v, str) else f"CAST('{v}' AS TIMESTAMP)"
                    for k, v in audit_values.items()
                ]
            )

            sql = f"""
                INSERT INTO {table.upper()} ({", ".join(db_cols)})
                    VALUES ({placeholders}, {audit_fragments})
            """

            dataset = ds.dataset(source_dir, format=file_ext.casefold())
            for batch in dataset.to_batches(batch_size=50000):
                # Convert Arrow batch to a list of tuples for Oracle
                data = batch.select(parquet_cols).to_pylist()

                # Use executemany for bulk binding
                cur.executemany(sql, data)
                conn.commit()

    def sql(self, query: str) -> list[Sequence[Any]]:
        """
        Executes raw SQL using the package driver.
        Used for commands and small metadata fetches.
        """
        with self.get_connection() as conn, conn.cursor() as cur:
            LOG.debug("Executing SQL query", extra={"query": query})
            cur.execute(query)
            rows = cur.fetchall()
            return [tuple(row) for row in rows]

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """Fetched concurrently by Ray, but limited by the Manager's Session Lock."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                LOG.debug("Executing SQL query", extra={"query": query})
                cursor.execute(query)

                # Ensure we have a valid description (required for column names)
                if cursor.description is None:
                    return

                columns = [c[0] for c in cursor.description]

                while True:
                    rows = cursor.fetchmany(50_000)
                    if not rows:
                        break

                    yield pl.DataFrame(rows, schema=columns, orient="row")

            finally:
                cursor.close()

    def get_schema(self, fq_table: str) -> pl.DataFrame:
        schema, table_name = fq_table.split(".")
        query = f"""
            SELECT
                column_name,
                data_type,
                nullable,
                data_length,
                data_precision,
                data_scale
            FROM all_tab_columns
            WHERE owner = UPPER('{schema}')
            AND table_name = UPPER('{table_name}')
            ORDER BY column_id;
        """
        return pl.concat(self.fetch_df(query), how="vertical")
