from typing import Any, Generator, Sequence


from oracledb import Connection
import polars as pl

from libs.clients.database.base import DBClient


class OracleClient(DBClient):
    def __init__(self, **config: Any) -> None:
        super().__init__(**config)

    def connect(self) -> Connection:
        import oracledb
        from libs.clients.base import ClientCantConnect

        # Thin mode: no instant client required
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
            raise ClientCantConnect(str(e))

        return self._connection

    def _ping(self, conn: Connection) -> None:
        conn.ping()

    def get_load_strategy(self, table_name: str, num_partitions: int = 10) -> list[str]:
        """
        Uses ORA_HASH to create N virtual partitions without needing a PK.
        """
        queries = []
        for i in range(num_partitions):
            # ORA_HASH(rowid, N) creates N buckets based on physical location
            sql = f"""
                SELECT * FROM {table_name} 
                WHERE ORA_HASH(rowid, {num_partitions - 1}) = {i}
            """
            queries.append(sql)
        return queries

    def sql(self, query: str) -> list[Sequence[Any]]:
        """
        Executes raw SQL using the package driver.
        Used for commands and small metadata fetches.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                return [tuple(row) for row in rows]

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """Fetched concurrently by Ray, but limited by the Manager's Session Lock."""
        cursor = self.connect().cursor()
        try:
            cursor.execute(query)

            # Ensure we have a valid description (required for column names)
            if cursor.description is None:
                return

            columns = [c[0] for c in cursor.description]

            while True:
                rows = cursor.fetchmany(50_000)
                if not rows:
                    break

                # Optimized: Use DataFrame constructor with schema instead of list-of-dicts
                yield pl.DataFrame(rows, schema=columns, orient="row")

        except Exception as e:
            # If connection died, reset to None so next call reconnects
            self._connection = None
            raise e
        finally:
            cursor.close()

    def write_table(self, lf: pl.LazyFrame, table_name: str, batch_size: int = 100_000) -> None:
        """
        Streams LazyFrame in chunks and uses executemany for batch binds.
        """
        # 1. Get column names and build the INSERT statement
        columns = lf.columns
        placeholders = ", ".join([f":{i + 1}" for i in range(len(columns))])
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # 2. Iterate through the LazyFrame in batches
        # .iter_slices() prevents the 50M rows from hitting RAM at once
        for batch_df in lf.collect().iter_slices(n_rows=batch_size):
            data = batch_df.to_dicts()  # Convert small chunk to list of dicts/tuples
            cursor = self.connect().cursor()
            cursor.executemany(sql, [tuple(d.values()) for d in data])
            self.connect().commit()
