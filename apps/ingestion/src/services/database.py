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
    def __init__(self, name: str, account_id: str, **config: Any):
        super().__init__(name, account_id, **config)
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
    def promote_data(self, staging_table: str, target_table: str, mode: str):
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
    
    def stage_data(self, lf: pl.LazyFrame, target_table: str) -> str:
        staging_table = f"stg_{target_table}_{int(time.time())}"
        
        # Performance: Use 'UNLOGGED' for staging to skip WAL logging (faster for 50M rows)
        self.client.sql(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table} INCLUDING ALL)")
        
        # Use the client to stream the Polars LazyFrame into the staging table
        # This uses the ADBC/Copy protocol under the hood
        self.client.write_table(lf, staging_table)
        return staging_table

    def promote_data(self, staging_table: str, target_table: str, mode: str):
        if mode == "upsert":
            # Native Postgres 'ON CONFLICT' or 'MERGE'
            sql = f"INSERT INTO {target_table} SELECT * FROM {staging_table} ON CONFLICT ... "
        else:
            # Generic Transactional Swap
            sql = f"BEGIN; TRUNCATE {target_table}; INSERT INTO {target_table} SELECT * FROM {staging_table}; COMMIT;"
        
        self.client.sql(sql)
        self.client.sql(f"DROP TABLE {staging_table}")
        
ServiceFactory.register("oracle")
class OracleService(DatabaseService):
    def _init_client(self, **config: Any) -> Any:
        secret: Secret = config["password"]
        return OracleClient(
            user=config['user'],
            password=secret.resolve(sanitize=True),
            dsn=config['dsn']
        )
    
    def stage_data(self, lf: pl.LazyFrame, target: str) -> StagingResult:
        temp_table = f"STG_{target}_{int(time.time())}"
        # ... logic to create table and self.client.write_table(lf) ...
        return StagingResult(staging_table=temp_table, rows=0)

    def promote_data(self, result: StagingResult, context: WriteContext):
        """Idempotent Promotion via Transaction."""
        stg = result.staging_table
        tgt = context.target
        
        # Build partition filter
        filter_clause = f"WHERE {context.partition_col} = '{context.partition_value}'" if context.partition_col else ""
        
        # Idempotency logic: 
        # 1. DELETE existing data for that partition
        # 2. INSERT from staging
        # All inside one BEGIN/COMMIT block
        sql = f"""
        BEGIN
            DELETE FROM {tgt} {filter_clause};
            INSERT INTO {tgt} SELECT * FROM {stg};
            COMMIT;
            EXECUTE IMMEDIATE 'DROP TABLE {stg}';
        END;
        """
        self.client.sql(sql)

@ServiceFactory.register("clickhouse")
class ClickHouseService(DatabaseService):
    def promote_data(self, result: StagingResult, context: WriteContext):
        """Idempotent Promotion via Partition Swap."""
        # ClickHouse has a native atomic command for this
        if context.load_mode in ["overwrite", "append"]:
            sql = f"ALTER TABLE {context.target} REPLACE PARTITION '{context.partition_value}' FROM {result.staging_table}"
            self.client.sql(sql)