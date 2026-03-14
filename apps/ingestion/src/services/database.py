from typing import Any, Generator, Callable, TypeVar
from abc import abstractmethod
import time

import polars as pl


from src.services.factory import ServiceFactory
from src.services.base import Service
from src.services.registry import protect_service

from libs.auth.models import Secret
from libs.clients.database.base import DBClient
from libs.clients.database.postgres import PostgresClient
from libs.clients.database.oracle import OracleClient, 
from libs.clients.database.clickhouse import ClickhouseClient



F = TypeVar("F", bound=Callable[..., Any])

# The Generic Fallback Logic
# If a database doesn't support a native "Atomic Swap" or "Merge," the fallback pattern should be:
# Stage: Create a temporary table and use standard batch inserts.
# Promote: Wrap a DELETE and INSERT INTO ... SELECT in a single SQL transaction.

class DatabaseService(Service):
    """
    Intermediate layer for all SQL-based sources.
    """
    def __init__(self, name: str, **config: Any):
        super().__init__(name, **config)
        self.client: DBClient = self._init_client(**config)

    @abstractmethod
    def _init_client(self, **config: Any) -> Any:
        """Subclasses must initialize their specific DB client."""
        pass
    
    def get_work_units(self, target: str, partitions: int) -> list[str]:
        # All DBs use the client's load strategy (e.g., ORA_HASH, ctid)
        return self.client.get_load_strategy(target, partitions)
    
    @protect_service(threshold=3) # type: ignore
    def sql(self, query: str) -> list[tuple[Any, ...]]:
        """
        Executes a standard SQL query and returns a Polars DataFrame.
        Used for smaller metadata queries or status checks.
        """
        return self.client.sql(query)
    
    @protect_service(threshold=3) # type: ignore
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        # Centralized protected fetch for all DB types
        return self.client.fetch_df(query)

    @abstractmethod
    def stage_data(self, df: pl.LazyFrame, target_table: str) -> str:
        """Phase 1: Returns the name of the temporary staging table."""
        pass

    @abstractmethod
    def promote_data(
        self, 
        staging_table: str, 
        target_table: str, 
        partition_col: str, 
        partition_val: str
    ) -> None:
        """Phase 2: Moves data to production (Swap/Merge/Append)."""
        pass

@ServiceFactory.register("postgres")
class PostgresService(DatabaseService):
    def __init__(self, name: str, **config: Any) -> None:
        super().__init__(name, **config)
        self.client = self._init_client(**config)

    def _init_client(self, **config: Any) -> PostgresClient:
        secret: Secret = config["password"]
        return PostgresClient(
            host=config["host"],
            db_name=config["database"],
            user=config["user"],
            password=secret.resolve(sanitize=True),
            port=config.get("port", 5432)
        )
    
    def stage_data(self, source_dir: str, target_table: str) -> str:
        staging_table = f"stg_{target_table}_{int(time.time())}"
        self.client.sql(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table})")
        
        # Polars scan_parquet handles a directory path natively.
        # It will treat all parquet files in the folder as a single dataset.
        lf = pl.scan_parquet(f"{source_dir}/*.parquet")
        
        # 1. Get the raw connection from the DBAPI
        conn = self.client.connect().raw_connection()
        
        try:
            with conn.cursor() as cursor:
                # 2. Open the COPY pipe
                copy_sql = f"COPY {staging_table} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"
                
                with cursor.copy(copy_sql) as copy:
                    # Stream in 100k chunks to keep RAM flat
                    for batch_df in lf.collect().iter_slices(n_rows=100_000):
                        # write_csv returns bytes, which we feed into the copy pipe
                        copy.write(batch_df.write_csv(include_header=False))
            
            # Commit only if the entire 50M row stream succeeded
            conn.commit()
            return staging_table
        except Exception as e:
            conn.rollback()
            self.client.reconnect()
            raise e

    def promote_data(
        self, 
        staging_table: str, 
        target_table: str, 
        partition_col: str, 
        partition_val: str
    ) -> None:
        # Transactional Swap
        sql = f"""
        BEGIN;
        DELETE FROM {target_table} 
            WHERE {partition_col} = '{partition_val}';
        INSERT INTO {target_table} 
            SELECT * FROM {staging_table};
        COMMIT;
        DROP TABLE {staging_table};
        """
        self.client.sql(sql)
        
ServiceFactory.register("oracle")
class OracleService(DatabaseService):
    def _init_client(self, **config: Any) -> Any:
        secret: Secret = config["password"]
        return OracleClient(
            user=config['user'],
            password=secret.resolve(sanitize=True),
            dsn=config['dsn']
        )
    
    def stage_data(self, source_dir: str, target_table: str) -> str:
        staging_table = f"STG_{target_table}"
    
        # Oracle 'ORACLE_BIGDATA' driver can read all files in a location
        # if the location is defined as a directory or a specific URI pattern
        sql = f"""
        CREATE TABLE {staging_table} (
            -- Schema columns
        )
        ORGANIZATION EXTERNAL (
            TYPE ORACLE_BIGDATA
            ACCESS PARAMETERS (
                com.oracle.bigdata.fileformat=parquet
            )
            -- Oracle allows wildcards in the location for BigData driver
            LOCATION ('{source_dir}/*.parquet')
        )
        REJECT LIMIT UNLIMITED
        """
        self.client.sql(sql)
        return staging_table

    def promote_data(
        self, 
        staging_table: str, 
        target_table: str, 
        partition_col: str, 
        partition_val: str
    ) -> None:
        # If partition_val is '2026-03-10', we wipe that day and replace it
        sql = f"""
        BEGIN
            -- Idempotency: Clear the target slice
            DELETE FROM {target_table}
            WHERE {partition_col} = '{partition_val};
            
            -- Performance: Use APPEND hint for direct-path insert (bypasses buffer cache)
            INSERT /*+ APPEND */ INTO {target_table} 
            SELECT * FROM {staging_table};
            
            COMMIT;
            EXECUTE IMMEDIATE 'DROP TABLE {staging_table}';
        EXCEPTION WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
        END;
        """
        self.client.sql(sql)

@ServiceFactory.register("clickhouse")
class ClickHouseService(DatabaseService):
    def stage_data(self, source_dir: str, target_table: str) -> str:
        staging_table = f"stg_{target_table}_{int(time.time())}"
        try:
            self.client.sql(f"CREATE TEMPORARY TABLE {staging_table} AS {target_table}")
            
            # ClickHouse pulls the folder directly - no Python RAM used
            path_pattern = f"{source_dir.rstrip('/')}/*.parquet"
            sql = f"INSERT INTO {staging_table} SELECT * FROM file('{path_pattern}', 'Parquet')"
            
            self.client.sql(sql)
            return staging_table
            
        except Exception:
            # Cleanup staging on failure to prevent orphan temp tables
            self.client.sql(f"DROP TABLE IF EXISTS {staging_table}")
            raise
    
    def promote_data(
        self, 
        staging_table: str, 
        target_table: str, 
        partition_col: str, 
        partition_val: str
    ) -> None:
        """
        Atomic metadata swap. 
        ClickHouse moves the actual data parts on disk
        Note: {partition_val} must match the internal ClickHouse partition ID format.
        """
        sql = f"""
            ALTER TABLE {target_table} 
            REPLACE PARTITION '{partition_val}' 
            FROM {staging_table}
        """
        try:
            self.client.sql(sql)
        finally:
            # Always drop the staging table after the swap attempt
            self.client.sql(f"DROP TABLE IF EXISTS {staging_table}")