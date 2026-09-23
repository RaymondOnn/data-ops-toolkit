"""Data loading and promotion strategies."""

from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

import polars as pl
from libs.utils.dates import current_timestamp
from loguru import logger
from msgspec import Struct

from src.core.stages.write.enums import WriteStrategy
from src.services.contracts import Sink
from src.services.database import DatabaseSink

if TYPE_CHECKING:
    from src.services.repo.metadata import MetadataRepository


LOG = logger
OUTPUT_FORMAT = "parquet"

# T_Sink must be Sink or any subclass/implementer of Sink
T_Sink = TypeVar("T_Sink", bound=Sink)


class WriteContext(Struct):
    """Context for data loading operations."""

    # partition_on: str | None
    # partition_value: str
    target: str  # Table name or path
    incoming_data_dir: str
    expected_count: int
    file_format: str = OUTPUT_FORMAT
    write_strategy: WriteStrategy = WriteStrategy.STAGING
    working_table: str | None = None


@runtime_checkable
class Writer(Protocol):
    """Abstract base class for data loaders."""

    def write(
        self,
        sink: T_Sink,
        context: WriteContext,
    ) -> tuple[str, int]:
        """Stage data from source directory to temporary location."""


class DataWriter:
    """Loader for database sinks (ClickHouse, Postgres, etc.)."""

    def write(
        self, sink: T_Sink, context: WriteContext, **kwargs: Any
    ) -> tuple[str, int]:
        """
        Stage data from source directory to temporary location.

        Args:
            sink: The sink to stage data to.
            source_dir: The source directory.
            context: The load context.
            file_ext: The file extension.
            audit: The audit data.

        Returns:
            tuple[str, int]: The staging ID and the number of files.
        """

        LOG.info(f"Staging data for {context.target}")

        def generate_table_name():
            timestamp = current_timestamp(naive=True).strftime("%Y%m%d%H%M%S")
            return f"{context.target}_stg_{timestamp}"

        match context.write_strategy:
            case WriteStrategy.DIRECT:
                dest_table = context.target
            case WriteStrategy.STAGING:
                dest_table = context.working_table or generate_table_name()
            case WriteStrategy.CLONE:
                dest_table = context.working_table or generate_table_name()
                # Runtime check for optional capability
                sink_fn = getattr(sink, "clone", None)
                if callable(sink_fn):
                    sink.clone(source=context.target, dest=dest_table)
                else:
                    raise NotImplementedError(
                        f"Sink type '{type(sink).__name__}' does not support WriteStrategy.CLONE"
                    )
            case _:
                raise ValueError(
                    f"Unsupported WriteStrategy type: {context.write_strategy}"
                )

        try:
            destination, rows = sink.stage(
                source_dir=context.incoming_data_dir,
                target=dest_table,
                file_format=context.file_format,
                expected_count=context.expected_count,
                **kwargs,
            )
            return destination, rows
        except Exception:
            LOG.exception("Staging failed")
            if context.write_strategy == WriteStrategy.CLONE:
                sink.delete(dest_table)
                LOG.warning(f"Cleaned up staging table: {dest_table}")
            raise

    def reconcile_schema(
        self,
        *,
        sink: Sink,
        meta_repo: "MetadataRepository",
        target: str,
        create_table_ddl: str | None,
        add_new_columns: bool,
        incoming_schema: dict[str, pl.DataType],
        metadata_columns: set[str] | list[str] | None = None,
        run_id: str | None = None,
    ) -> list[str]:
        if not isinstance(sink, DatabaseSink):
            return list(incoming_schema.keys())

        from libs.database import TypeResolver
        from libs.database.exceptions import MissingColumns

        dialect = sink.connector.dialect
        meta_cols_set = set(metadata_columns or [])

        # 1. Create table if missing (includes all columns: user + metadata)
        if not sink.exists(target):
            LOG.info(f"Table '{target}' does not exist. Creating...")
            db_schema = {
                col: TypeResolver.polars_to_db_type(dialect, dtype)
                for col, dtype in incoming_schema.items()
            }
            sink.setup_resource(
                target=target, schema_columns=db_schema, ddl=create_table_ddl
            )

            meta_repo.record_schema_change(
                target_table=target,
                action="CREATE_TABLE",
                run_id=run_id,
                schema_json=db_schema,
            )

        # 2. Get existing table catalog schema
        existing_schema_df = sink.connector.get_schema(target)
        existing_col_names = (
            set(existing_schema_df["column_name"].to_list())
            if not existing_schema_df.is_empty()
            and "column_name" in existing_schema_df.columns
            else set()
        )

        incoming_col_names = set(incoming_schema.keys())

        # 3. Check for missing columns in target table
        missing_in_data = existing_col_names - incoming_col_names
        if missing_in_data:
            raise MissingColumns(
                target=target, missing=missing_in_data, available=incoming_col_names
            )

        # 4. Handle new incoming columns (ALTER TABLE)
        new_cols = [col for col in incoming_schema if col not in existing_col_names]
        if not new_cols:
            return list(incoming_schema.keys())

        # Partition new columns into system metadata vs. source data
        new_meta_cols = [col for col in new_cols if col in meta_cols_set]
        new_user_cols = [col for col in new_cols if col not in meta_cols_set]

        def _add_column_to_sink(col_name: str) -> None:
            polars_dtype = incoming_schema[col_name]
            data_type = TypeResolver.polars_to_db_type(
                dialect=dialect, dtype=polars_dtype
            )
            sink.add_column(
                target=target, column_name=col_name, data_type=str(data_type)
            )
            meta_repo.record_schema_change(
                target_table=target,
                action="ADD_COLUMN",
                run_id=run_id,
                column_name=col_name,
                data_type=str(data_type),
            )

        # Always evolve target table for internal metadata columns
        if new_meta_cols:
            LOG.info(
                f"Evolving target table '{target}'. Adding missing system metadata columns: {new_meta_cols}"
            )
            for col in new_meta_cols:
                _add_column_to_sink(col)

        # Apply add_new_columns gate only to source/user columns
        if new_user_cols:
            if add_new_columns:
                LOG.info(
                    f"add_new_columns=True. Evolving target table '{target}'. "
                    f"Adding missing user columns: {new_user_cols}"
                )
                for col in new_user_cols:
                    _add_column_to_sink(col)
            else:
                LOG.warning(
                    f"add_new_columns=False. Skipping schema evolution. "
                    f"Ignoring incoming user columns not present in target table '{target}': {new_user_cols}"
                )
                # Filter out rejected user columns from the final list
                return [col for col in incoming_schema if col not in set(new_user_cols)]

        return list(incoming_schema.keys())


# class FileWriter(Writer):
#     """Loader for file-based sinks (S3, local filesystem)."""

#     def stage(
#         self,
#         sink: Sink,
#         source_dir: Path,
#         context: LoadContext,
#         file_ext: str = "parquet",
#         audit: dict[str, Any] | None = None,
#     ) -> tuple[str, int]:
#         """
#         Stage files to temporary location.

#         Args:
#             sink: The sink to stage data to.
#             source_dir: The source directory.
#             context: The load context.
#             file_ext: The file extension.
#             audit: The audit data.

#         Returns:
#             tuple[str, int]: The staging ID and the number of files.
#         """
#         LOG.info(f"Staging files to {context.target}")

#         # For file sinks, stage returns (staging_path, file_count)
#         result = sink.stage(
#             source_dir=source_dir,
#             target=context.target,
#             file_ext=file_ext,
#             expected_count=context.expected_count,
#             audit_values=audit,
#         )

#         if result is None:
#             raise ValueError(f"File stage failed for {type(sink).__name__}")

#         return result

#     def promote(
#         self,
#         sink: Sink,
#         staging_id: str,
#         context: LoadContext,
#     ) -> None:
#         """
#         Move staged files to final destination.

#         Args:
#             sink: The sink to promote data to.
#             staging_id: The staging ID.
#             context: The load context.
#         """
#         LOG.info(f"Promoting {staging_id} -> {context.target}")

#         # For file sinks, promote moves/copies files to final location
#         sink.promote(
#             staging=staging_id,
#             target=context.target,
#             partition_on=context.partition_on,
#             partition_value=context.partition_value,
#             expected_count=context.expected_count,
#         )


# # Factory for getting the appropriate loader
# class LoaderFactory:
#     """Factory for creating loaders based on sink type."""

#     _LOADERS: ClassVar[dict[str, type[Writer]]] = {
#         "data": DataWriter,
#         "blob": FileWriter,
#     }

#     @classmethod
#     def get_loader(cls, sink_type: str) -> Loader:
#         """
#         Get loader for the specified sink type.

#         Args:
#             sink_type: The type of the sink.

#         Returns:
#             Loader: The loader for the specified sink type.
#         """
#         loader_cls = cls._LOADERS.get(sink_type, DataWriter)
#         return loader_cls()
