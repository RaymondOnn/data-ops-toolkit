import logging
from collections.abc import Generator, Sequence
from typing import Any

import polars as pl
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

        if self._connection:
            return self._connection

        try:
            self._connection = oracledb.connect(
                user=self.config["user"],
                password=self.config["password"],
                dsn=self.config["dsn"],
            )
            self._ping(self._connection)
        except Exception as e:
            raise ClientCantConnect("Failed to connect to Oracle") from e

        return self._connection

    def _ping(self, conn: Connection) -> None:
        conn.ping()

    def get_load_strategy(
        self,
        table_name: str,
        num_partitions: int = 10,
        filter_sql: str | None = None,
    ) -> set[str]:
        """
        Uses ORA_HASH to create N virtual partitions without needing a PK.
        """
        filter_sql = filter_sql.replace("WHERE", "") if filter_sql else ""
        queries = []
        for i in range(num_partitions):
            # ORA_HASH(rowid, N) creates N buckets based on physical location
            sql = f"""
                SELECT * FROM {table_name} 
                WHERE {filter_sql} 
                AND ORA_HASH(rowid, {num_partitions - 1}) = {i}
            """
            queries.append(sql)
        return set(queries)

    def sql(self, query: str) -> list[Sequence[Any]]:
        """
        Executes raw SQL using the package driver.
        Used for commands and small metadata fetches.
        """
        with self.connect() as conn, conn.cursor() as cur:
            LOG.debug("Executing SQL query", extra={"query": query})
            cur.execute(query)
            rows = cur.fetchall()
            return [tuple(row) for row in rows]

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """Fetched concurrently by Ray, but limited by the Manager's Session Lock."""
        cursor = self.connect().cursor()
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

        except Exception as e:
            # If connection died, reset to None so next call reconnects
            self._connection = None
            raise e
        finally:
            cursor.close()

    # def write_table(
    #     self, lf: pl.LazyFrame, table_name: str, batch_size: int = 100_000
    # ) -> None:
    #     """
    #     Streams LazyFrame in chunks and uses executemany for batch binds.
    #     """
    #     # 1. Get column names and build the INSERT statement
    #     columns = lf.columns
    #     placeholders = ", ".join([f":{i + 1}" for i in range(len(columns))])
    #     sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    #     # 2. Iterate through the LazyFrame in batches
    #     # .iter_slices() prevents the 50M rows from hitting RAM at once
    #     for batch_df in lf.collect().iter_slices(n_rows=batch_size):
    #         data = batch_df.to_dicts()  # Convert small chunk to list of dicts/tuples
    #         cursor = self.connect().cursor()
    #         cursor.executemany(sql, [tuple(d.values()) for d in data])
    #         self.connect().commit()

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
