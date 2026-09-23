"""Base database service abstractions.

This module provides the structural foundation for all SQL-based services,
handling connection pooling, resource-aware partitioning, and circuit
breaker integration to ensure resilient data extraction and loading.
"""

import re
from collections.abc import Generator
from copy import deepcopy
from typing import Any

import polars as pl
from libs.auth.secret import Secret
from libs.clients.base import ClientCantConnect
from libs.database import DatabaseConnector
from libs.database.sql import Predicate, SQLContext
from libs.resilience.circuit_breaker import CircuitBreaker
from libs.utils.dates import current_timestamp
from loguru import logger

from src.services.base import Service
from src.services.contracts import Sink, Source
from src.services.health.monitor import monitor
from src.utils.constants import STRIP_TZ_FOR_DB

LOG = logger
MAX_CELLS_PER_WORKER = 20_000_000
MIN_ROWS_PER_WORKER = 50_000

breaker = CircuitBreaker(
    failure_threshold=3,
    timeout_secs=300,
    tracked_exceptions=(ClientCantConnect, ConnectionError, TimeoutError),
)


@Service.register()
class DatabaseService(Service):
    """Base class for SQL-based ingestion services.

    Provides lazily-initialized database clients, connection management via
    context managers, and standardized telemetry through circuit breakers.

    Attributes:
        config (dict): Configuration parameters including credentials.
    """

    def __init__(self, name: str | None = None, **config: Any):
        """Initializes the SQL-based service with registry metadata.

        Args:
            name: Name of the service instance.
            **config: Driver-specific configuration parameters, including
                credentials which may be `Secret` objects.
        """
        self.name = name or config.get("type") or self.__class__.__name__.lower()
        self._config = config

    def probe(self, target: str | None = None) -> bool:
        """Health probe for database connector."""
        try:
            res = self.connector.query("SELECT 1")
            return bool(res)
        except Exception as e:
            LOG.warning(f"Database health probe failed for {self.name}: {e}")
            return False

    def reset_client(self) -> None:
        """Invalidates the current database client.

        Used by the circuit breaker to force a full re-connection if the
        underlying driver's connection pool becomes stale or corrupted
        after a network interruption.
        """
        if "client" in self.__dict__:
            LOG.warning(f"Resetting database client for {self.__class__.__qualname__}")
            self.__dict__.pop("client", None)

    @property
    def connector(self) -> "DatabaseConnector":
        """Retrieves the database client, initializing it if necessary.

        Connections are not established until the first operation is requested,
        reducing overhead for short-lived metadata checks.

        Returns:
            DBClient: The protocol-specific database client.
        """
        resolved_config = deepcopy(self._config)
        for key, value in self._config.items():
            if isinstance(value, Secret):
                resolved_config[key] = value.resolve(url_encode=True)

        return DatabaseConnector(dialect=self._config["db_type"], **resolved_config)

    @monitor(breaker)
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """Streams query results as a sequence of Polars DataFrames.

        By yielding batches instead of a single object, we can process datasets
        larger than the worker's RAM without triggering OOM events.

        Args:
            query: The SQL query to execute.

        Yields:
            pl.DataFrame: A chunk of the result set.
        """
        return self.connector.fetch_df(query)

    def count_units(self, target: str, filter_condition: str | None = None) -> int:
        """Abstract method for determining resource volume (row count).

        Args:
            target: The resource identifier (table name).
            filter_condition: Optional SQL WHERE clause.

        Returns:
            int: Total row count.
        """
        where = (
            re.sub(r"(?i)^where\s+", "", filter_condition.strip())
            if filter_condition
            else "1=1"
        )
        where_cond = self.connector.where(where)
        query = self.connector.select(table=target, where_cond=where_cond)
        return self._count_rows(query)

    def _count_rows(self, query: str) -> int:
        """
        Counts rows for a target table or a complex query (including CTEs).
        """
        clean_query = query.strip().rstrip(";")

        # 1. Compile dialect-specific count query
        count_sql = self.connector.build_sql("count", table=clean_query)

        try:
            # 2. Execute via monitored fetch_df generator and grab the first DataFrame batch
            df = next(self.fetch_df(count_sql), None)
            if df is not None and not df.is_empty():
                # 3. Extract single count scalar cleanly using Polars .item()
                return int(df.item(0, 0))

            return 0
        except Exception:
            LOG.exception(f"Count failed for target query/table: {query[:50]}...")
            return 0

    @monitor(breaker)
    def exists(self, target: str) -> bool:
        """Checks if a table or view exists in the database."""
        sql = self.connector.build_sql("table_exists", table_name=target)
        result = self.connector.query(sql)
        first_row = next(result, None)
        if not first_row:
            return False
        return (
            bool(first_row[0])
            if isinstance(first_row, list | tuple)
            else bool(first_row)
        )

    def setup_resource(self, target: str, **kwargs: Any) -> None:
        """Creates a table using DDL string or schema dictionary."""
        if (ddl := kwargs.get("ddl")) is not None:
            self.connector.db.command(ddl)
        else:
            schema_columns: dict[str, str] = kwargs.get("schema_columns", {})
            ddl = self.connector.build_sql(
                operation="create_table",
                table_name=target,
                schema_columns=schema_columns,
            )
        self.connector.command(ddl)
        LOG.info(f"Created table: {target}")

    def add_column(self, target: str, column_name: str, data_type: str) -> None:
        """Appends a new column to the target table."""
        sql = self.connector.build_sql(
            operation="add_column",
            table_name=target,
            column_name=column_name,
            data_type=data_type,
        )
        self.connector.command(sql)
        LOG.info(f"Added column {column_name} ({data_type}) to {target}")

    def delete(self, target: str) -> None:
        sql = self.connector.build_sql("drop_table", table_name=target)
        self.connector.command(sql)
        LOG.warning(f"Dropped table: {target}")

    def clone(self, source: str, dest: str) -> None:
        """Clones a table structure for regression testing."""
        sql = self.connector.build_sql("like_table", src_table=source, tgt_table=dest)
        self.connector.command(sql)
        LOG.info(f"Cloned {source} -> {dest}")

    def _get_schema(self, target) -> pl.DataFrame:
        """
        Returns polars schema resolved via TypeResolver
        """
        return self.connector.get_schema(target)


@Service.register()
class DatabaseSource(DatabaseService, Source):
    """Service for extracting data from SQL databases."""

    def parallelize(
        self,
        target: str,
        sql_context: SQLContext,
        num_workers: int | None = None,
        filter_condition: str | None = None,
        **kwargs: Any,
    ) -> list[str]:
        """Calculates optimal parallel partitions based on a Cell-Budget heuristic.

        Instead of splitting by row count alone, we calculate the total volume
        of 'cells' (rows x columns). We target ~20M cells per worker, which
        keeps memory usage within the 2GB Ray worker limit while maximizing
        throughput.

        Args:
            target: Table name to partition.
            num_workers: Manual worker count override.
            filter_condition: Optional SQL WHERE clause.
            **kwargs: Contextual params (e.g., 'schema').

        Returns:
            set[str]: A collection of parallel SQL queries.
        """
        incremental_predicate = kwargs.get("incremental_predicate")

        # 1. Update SQLContext with the merged incremental predicate
        if sql_context and sql_context.main:
            existing_where = sql_context.main.where
            if existing_where and existing_where.strip() != "1=1":
                sql_context.main.where = (
                    f"({existing_where}) AND ({incremental_predicate})"
                )
            else:
                sql_context.main.where = incremental_predicate

            compiled_query = self.connector.sql.compile_context(sql_context)
            select_cols = sql_context.main.select
            num_columns = (
                len(select_cols)
                if select_cols
                else self.connector.get_schema(target).height
            )
        else:
            where_clause = (
                f"WHERE {incremental_predicate}"
                if incremental_predicate != "1=1"
                else ""
            )
            compiled_query = f"SELECT * FROM {target} {where_clause}".strip()
            num_columns = self.connector.get_schema(target).height

        # 2. Compute worker allocation based on row/cell counts
        total_rows = self._count_rows(compiled_query)
        total_cells = total_rows * num_columns

        if num_workers:
            # If workers are provided, we check if the resulting chunks
            # violate our memory safety ceiling (20M cells).
            cells_per_worker = total_cells // num_workers
            if cells_per_worker > MAX_CELLS_PER_WORKER:
                suggested = total_cells // MAX_CELLS_PER_WORKER
                LOG.warning(
                    f"Manual worker count ({num_workers}) may cause OOM. "
                    f"Each worker will handle {cells_per_worker:_} cells. "
                    f"Suggested workers for this width: {suggested}",
                    source=self.__class__.__qualname__,
                )
        else:
            # 1. Calculate ideal worker count based on cell memory budget
            num_workers = total_cells // MAX_CELLS_PER_WORKER

        # 2. Convert back to rows to ensure whole rows per worker
        if num_workers > 0:
            rows_per_worker = total_rows // num_workers
            if rows_per_worker < MIN_ROWS_PER_WORKER:
                num_workers = total_rows // MIN_ROWS_PER_WORKER

        # 3. Final safety clamping (1 to 100 workers)
        num_workers = max(1, num_workers)

        LOG.info(
            f"DB Partitioning: {total_rows:_} rows, {num_columns} cols "
            f"({total_cells:_} total cells). Using {num_workers} workers.",
            source=self.__class__.__qualname__,
        )

        return self._partition_load(
            table_or_query=compiled_query,
            num_workers=num_workers,
            partition_fields=kwargs.get("primary_keys"),
        )

    def _partition_load(
        self,
        table_or_query: str,
        num_workers: int,
        partition_fields: str | list[str] | None = None,
    ):
        """
        Generates partitioned SQL queries using cityHash64 for parallel loading.
        Safely handles both plain tables and complex CTE blocks by wrapping them in subqueries.

        Args:
            query: The fully compiled SQL query string or table expression.
            num_workers: Number of workers/partitions to split across.

        Returns:
            set[str]: A set of partitioned query strings.
        """
        if num_workers <= 0:
            raise ValueError("num_workers must be an integer greater than 0.")

        clean_query = table_or_query.strip().rstrip(";")

        # Wrap the incoming query inside a subquery parentesis block.
        # This prevents CTE declarations from clashing with the outer WHERE clause.
        # Note: Hash primary key / sort keys when possible

        # Format partition key fields into dialect-specific hash expression (e.g. cityHash64("id", "tenant"))
        if partition_fields:
            p_fields = (
                [partition_fields]
                if isinstance(partition_fields, str)
                else partition_fields
            )
        else:
            p_fields = "*"
        hash_expr = self.connector.expr("hash", fields=p_fields)

        queries = []
        for i in range(num_workers):
            partition_cond = {f"{hash_expr} % {num_workers}": i}
            where_cond = self.connector.where(partition_cond)
            compiled_query = self.connector.select(
                table=clean_query, where_cond=where_cond
            )
            queries.append(compiled_query)

        return queries

    def resolve_identity(
        self, target: str, items: list[str] | None = None, **kwargs
    ) -> str:
        """Resolves the audit identity for the source resource."""
        return target

    @monitor(breaker)
    def pull(self, unit: str, **kwargs: Any) -> Generator[pl.DataFrame, None, None]:
        """Streams data for a specific work unit as Arrow-backed Polars chunks.

        Passes the native Arrow RecordBatch stream from the DB driver directly
        to the caller without materialising the full partition. Each yielded
        DataFrame corresponds to one driver-level RecordBatch, keeping
        per-worker peak memory to a single batch rather than the whole partition.

        Args:
            unit: The partitioned SQL query to execute.

        Yields:
            pl.DataFrame: One RecordBatch-sized chunk of the partition.
        """
        yield from self.connector.fetch_df(unit)


@Service.register()
class DatabaseSink(DatabaseService, Sink):
    def stage(
        self,
        source_dir: str,
        target: str,
        expected_count: int,
        file_format: str = "parquet",
        **kwargs: Any,
    ) -> tuple[str, int]:
        """Phase 1: Loads local artifacts into a temporary database table.

        Args:
            source_dir: Directory containing transformed Parquet files.
            target: Production table name.
            expected_count: Row count verification.
            file_ext: Format of files in source_dir.
            audit_values: Metadata to inject into the table.
        """
        try:
            self.connector.copy_from_file(
                table=target,
                source_dir=source_dir,
                file_format=file_format,
                columns=kwargs.get("columns"),
            )

            # Verify row count
            select_sql = self.connector.select(table=target)
            rows = self._count_rows(query=select_sql)
            if rows != expected_count:
                raise ValueError(
                    f"Row count mismatch: expected {expected_count}, got {rows}"
                )
            LOG.success(f"Staged data in '{target}'")
            return target, rows

        except Exception:
            LOG.exception("Staging failed")
            raise

    def promote(
        self,
        source: str,
        destination: str,
        expected_count: int,
        **kwargs: Any,
    ) -> None:
        """Phase 2: Promotes data from staging to production.

        Args:
            source: The source table identifier.
            target: The production table identifier.
            partition_on: The column used for partitioning logic.
            partition_value: The value to overwrite.
            expected_count: Final count verification.
        """
        merge_ops = kwargs.get("merge_ops")
        update_key = kwargs.get("update_key")
        partition_value = kwargs.get("partition_value")
        primary_keys = kwargs.get("primary_keys")
        soft_delete_missing = kwargs.get("soft_delete_missing")
        soft_delete_column = kwargs.get("soft_delete_column")
        now = current_timestamp(naive=STRIP_TZ_FOR_DB)

        # if not (update_key and partition_value):
        #     raise ValueError("")

        if not merge_ops:
            raise ValueError("Parameter 'merge_ops' is required.")

        # 1. Engine-agnostic schema safety check
        self._validate_schema_compatibility(source=source, destination=destination)

        # 2. Schema check for soft-delete column presence
        if soft_delete_missing:
            dest_schema = self.connector.get_schema(destination)
            dest_cols = (
                set(dest_schema["column_name"].to_list())
                if "column_name" in dest_schema.columns
                else set()
            )
            if soft_delete_column not in dest_cols:
                raise ValueError(
                    f"Cannot execute soft_delete_missing: column '{soft_delete_column}' "
                    f"does not exist in target table '{destination}'."
                )

            # Append soft_delete operation to execution plan
            if "soft_delete" not in merge_ops:
                merge_ops = [*list(merge_ops), "soft_delete"]

        success = False
        try:
            self._apply_merge_operation(
                target=destination,
                source=source,
                merge_ops=merge_ops,
                update_key=update_key,
                partition_value=partition_value,
                primary_keys=primary_keys,
                soft_delete_column=str(soft_delete_column),
                current_timestamp=now.strftime("%Y-%m-%d %H:%M:%S"),
            )

            # Verify row count
            # predicate: list[Predicate] = [f"{update_key} = {partition_value}"]
            self._verify_promotion(
                source=source,
                destination=destination,
                expected_count=expected_count,
                merge_ops=merge_ops,
                update_key=update_key,
                partition_value=partition_value,
                soft_delete_column=soft_delete_column,
            )

            # promoted = self._count_rows(query=select_sql)
            # if promoted != expected_count:
            #     raise ValueError(
            #         f"Row count mismatch after promotion: "
            #         f"expected {expected_count}, got {promoted}"
            #     )
            success = True
            LOG.success(f"Promoted {update_key}={partition_value} to {destination}")

        finally:
            if success:
                self.delete(source)
                LOG.info(f"Cleaned up source: {source}")

    def _validate_schema_compatibility(self, source: str, destination: str) -> None:
        """Validates column compatibility between staging source and target destination."""
        if not self.exists(destination):
            raise ValueError(
                f"Target destination table '{destination}' does not exist."
            )

        # Extract column sets directly from Polars schemas
        src_cols = set(self.connector.get_schema(source)["column_name"])
        tgt_cols = set(self.connector.get_schema(destination)["column_name"])

        missing_in_staging = tgt_cols - src_cols
        if missing_in_staging:
            raise ValueError(
                f"Schema mismatch between '{source}' and '{destination}'. "
                f"Staging table is missing required target columns: {missing_in_staging}"
            )

    def _build_join_condition(self, target: str, source: str, pks: list[str]) -> str:
        """Helper to construct identifier-quoted join predicates: tgt."pk" = src."pk"."""
        return " AND ".join(
            f"tgt.{self.connector.sql.quote_identifier(pk)} = {self.connector.sql.quote_identifier(source)}.{self.connector.sql.quote_identifier(pk)}"
            for pk in pks
        )

    def _apply_merge_operation(
        self,
        *,
        target: str,
        source: str,
        merge_ops: list[str] | tuple[str, ...],
        primary_keys: str | list[str] | None = None,
        update_key: str | None = None,
        partition_value: Any | None = None,
        soft_delete_column: str = "_is_deleted",
        current_timestamp: str,
    ) -> None:
        pks = [primary_keys] if isinstance(primary_keys, str) else (primary_keys or [])

        # 1. Inspect source schema ONCE for all operation steps
        staging_schema_df = self.connector.get_schema(source)
        all_columns = (
            staging_schema_df["column_name"].to_list()
            if "column_name" in staging_schema_df.columns
            else []
        )

        for operation in merge_ops:
            LOG.debug(f"Processing merge step '{operation}' for table '{target}'")

            match operation:
                case "truncate":
                    sql = self.connector.build_sql("truncate", table_name=target)

                case "delete":
                    if not update_key or partition_value is None:
                        raise ValueError(
                            "Operation 'delete' requires 'update_key' and 'partition_value'."
                        )
                    where_cond = self.connector.where(
                        (str(update_key), "=", partition_value)
                    )
                    sql = self.connector.build_sql(
                        "merge_delete", tgt_table=target, where_cond=where_cond
                    )

                case "update":
                    if not pks:
                        raise ValueError(
                            "Primary key(s) required for 'update' operation."
                        )
                    update_columns = [col for col in all_columns if col not in pks]
                    sql = self.connector.build_sql(
                        "merge_update",
                        tgt_table=target,
                        src_table=source,
                        join_cond=pks,
                        set_values=update_columns,
                    )

                case "insert":
                    where_cond = None
                    # Anti-join for upserts (update + insert): exclude records already existing in target
                    if "update" in merge_ops and pks:
                        join_str = self._build_join_condition(target, source, pks)
                        anti_join_subquery = f"SELECT 1 FROM {self.connector.sql.quote_identifier(target)} AS tgt WHERE {join_str}"
                        where_cond = ("NOT EXISTS", anti_join_subquery)

                    sql = self.connector.build_sql(
                        "merge_insert",
                        tgt_table=target,
                        src_table=source,
                        fields=all_columns,
                        where_cond=where_cond,
                    )

                case "soft_delete":
                    if not pks or not update_key or partition_value is None:
                        raise ValueError(
                            "'soft_delete' requires primary_keys, update_key, and partition_value."
                        )

                    pk_subquery = self.connector.select(table=source, fields=pks)
                    pk_expr = (
                        self.connector.sql.quote_identifier(pks[0])
                        if len(pks) == 1
                        else f"({', '.join(self.connector.sql.quote_identifier(pk) for pk in pks)})"
                    )

                    scoped_conds: list[Predicate] = [
                        (str(update_key), "=", partition_value),
                        f"{self.connector.sql.quote_identifier(soft_delete_column)} IS NULL",
                        f"{pk_expr} NOT IN ({pk_subquery})",
                    ]
                    sql = self.connector.build_sql(
                        "update",
                        table_name=target,
                        set_values={soft_delete_column: current_timestamp},
                        where_cond=self.connector.where(scoped_conds),
                    )

                case _:
                    raise NotImplementedError(
                        f"Unsupported merge operation: {operation}"
                    )

            LOG.debug(f"Executing sql statement: {sql}")
            self.connector.command(sql)

    def _verify_promotion(
        self,
        *,
        destination: str,
        source: str,
        expected_count: int,
        merge_ops: list[str] | tuple[str, ...],
        primary_keys: str | list[str] | None = None,
        update_key: str | None = None,
        partition_value: Any | None = None,
        soft_delete_column: str | None = "_is_deleted",
    ) -> None:
        """Verifies promotion success based on the active merge strategy."""

        # Strategy 1: Truncate / Full Refresh
        if "truncate" in merge_ops:
            actual = self._count_rows(self.connector.select(table=destination))
            if actual != expected_count:
                raise ValueError(
                    f"Truncate promotion count mismatch: expected {expected_count}, got {actual}"
                )
            return

        # Strategy 2: Upsert (Update + Insert) -> Dynamic PK existence check
        if "update" in merge_ops and "insert" in merge_ops:
            pks = (
                [primary_keys]
                if isinstance(primary_keys, str)
                else (primary_keys or [])
            )
            if not pks:
                raise ValueError(
                    "Verification for 'update' merge requires primary_keys."
                )

            join_str = self._build_join_condition(destination, source, pks)
            pks_present_sql = f"""
                SELECT COUNT(1)
                FROM {self.connector.sql.quote_identifier(destination)} AS tgt
                WHERE EXISTS (
                    SELECT 1 FROM {self.connector.sql.quote_identifier(source)} AS {self.connector.sql.quote_identifier(source)}
                    WHERE {join_str}
                )
            """
            staged_pks_found = self._count_rows(pks_present_sql)
            if staged_pks_found != expected_count:
                raise ValueError(
                    f"Upsert promotion missing keys: expected {expected_count} keys in target, found {staged_pks_found}"
                )
            return

        # Strategy 3: Partition Delete + Insert / Overwrite
        if update_key and partition_value is not None:
            conds: list[Predicate] = [(str(update_key), "=", partition_value)]
            if "soft_delete" in merge_ops and soft_delete_column:
                conds.append(
                    f"{self.connector.sql.quote_identifier(soft_delete_column)} IS NULL"
                )

            actual = self._count_rows(
                self.connector.select(
                    table=destination, where_cond=self.connector.where(conds)
                )
            )
            if actual != expected_count:
                raise ValueError(
                    f"Partition promotion count mismatch for {update_key}={partition_value}: expected {expected_count}, got {actual}"
                )

    def is_equal(
        self,
        ref: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        """Compares two tables for data equality."""
        ref_rows = self._count_rows(self.connector.select(table=ref))
        other_rows = self._count_rows(self.connector.select(table=other))
        if ref_rows != other_rows:
            return False
        if self._get_checksum(ref) == self._get_checksum(other):
            return True
        return self._minus(ref, other, exclude_columns) == 0

    def _minus(
        self,
        ref: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> int:
        """Performs a SQL MINUS/EXCEPT to find data drift."""
        exclude = exclude_columns or set()
        describe_tbl_ref = self.connector.build_sql("describe_table", table_name=ref)
        describe_tbl_other = self.connector.build_sql(
            "describe_table", table_name=other
        )

        cols_ref = {
            row[0].decode() if isinstance(row[0], bytes) else row[0]
            for row in self.connector.query(describe_tbl_ref)
        }
        cols_other = {
            row[0].decode() if isinstance(row[0], bytes) else row[0]
            for row in self.connector.query(describe_tbl_other)
        }

        common = (cols_ref & cols_other) - exclude
        if not common:
            raise ValueError(
                f"No common columns found. " f"ref: {cols_ref}, other: {cols_other}"
            )

        minus_sql = self.connector.build_sql(
            "minus", ref_table=ref, other_table=other, fields=sorted(common)
        )
        count_sql = self.connector.build_sql("count", table=minus_sql)
        result = self.connector.query(count_sql)
        first_row = next(result, None)
        return int(first_row[0]) if first_row else 0

    def _get_checksum(self, name: str, columns: list[str] | None = None) -> str:
        """Generates a data fingerprint for the table."""
        try:
            field = self.connector.expr("hash", fields=columns)
            select_sql = self.connector.select(table=name, fields=[field])
            result = self.connector.query(select_sql)

            first_row = next(result, None)
            return str(first_row[0]) if first_row else "0"
        except Exception:
            LOG.exception(f"Checksum failed for {name}")
            return "ERROR"
