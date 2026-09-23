from datetime import datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from libs.database.sql.utils import format_sql_value
from loguru import logger
from msgspec import Struct, json

if TYPE_CHECKING:
    from src.services.repo.metadata import MetadataRepository

LOG = logger
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


class CheckpointType(StrEnum):
    """Supported checkpoint types across database, file, and API sources."""

    # Database / SQL Sources
    # PK_RANGE = "PK_RANGE"  # Primary key chunks (e.g., id >= 1000 AND id < 2000)
    # TIMESTAMP = "TIMESTAMP"  # Time-based watermarks (e.g., updated_at > '2026-01-01')
    UPDATE_KEY = "UPDATE_KEY"  # Monotonic sequence keys (e.g., sequence_id > 500)
    LSN_OFFSET = (
        "LSN_OFFSET"  # DB log stream positions (e.g., Postgres LSN, MySQL GTID)
    )

    # # File / Storage Sources
    # FILE = "FILE"  # File modification timestamps, path lists, or glob state

    # API / Webhook Sources
    CURSOR = (
        "CURSOR"  # Opaque API cursor string (e.g., Stripe/HubSpot pagination token)
    )
    PAGE_OFFSET = "PAGE_OFFSET"  # Page index / record offset pagination (e.g., page=5, offset=500)


# class CursorPayload(TypedDict):
#     next_cursor: str  # Clean, stateless token (e.g., Stripe/Hubspot pagination token)


# class PageOffsetPayload(TypedDict):
#     page: int
#     offset: int
#     limit: int


class Checkpoint(Struct):
    """Represents data ingestion watermark state and filter generator."""

    type: CheckpointType
    start_value: str | None = None
    end_value: str | None = None
    state_payload: dict[str, Any] | None = None

    @classmethod
    def load_latest(
        cls,
        meta_repo: "MetadataRepository | None",
        job_id: str,
        dataset_id: str,
        default_type: CheckpointType = CheckpointType.UPDATE_KEY,
    ) -> "Checkpoint":
        """Fetches latest dataset checkpoint record from repository.

        Args:
            meta_repo: Metadata storage repository instance.
            job_id: Identifier for target execution job.
            dataset_id: Identifier for dataset within job context.
            default_type: Fallback checkpoint type if unspecified.

        Returns:
            Checkpoint instance populated with high-water marks.
        """
        if not meta_repo:
            return cls(type=default_type)

        record = meta_repo.get_latest_checkpoint(job_id=job_id, dataset_id=dataset_id)
        if not record:
            return cls(type=default_type)

        payload_dict = None
        raw_payload = getattr(record, "state_payload", None)
        if raw_payload:
            try:
                payload_dict = json.decode(
                    (
                        raw_payload
                        if isinstance(raw_payload, bytes)
                        else raw_payload.encode("utf-8")
                    ),
                    type=dict[str, Any],
                )
            except Exception:
                payload_dict = None

        raw_type = getattr(record, "checkpoint_type", default_type)
        chk_type = (
            CheckpointType(raw_type)
            if raw_type in CheckpointType.__members__
            else default_type
        )

        return cls(
            type=chk_type,
            start_value=getattr(record, "checkpoint_end_value", None),
            state_payload=payload_dict,
        )

    def serialize_payload(self) -> str:
        """Encodes state payload dictionary to JSON string.

        Returns:
            UTF-8 encoded JSON string representation.
        """
        return json.encode(self.state_payload).decode("utf-8")

    def update_end_value(self, new_val: str | None) -> None:
        """Updates end watermark if new value exceeds current state.

        Args:
            new_val: Candidate high-water mark value to assess.
        """
        if new_val is None:
            return

        if self.end_value is None or new_val > self.end_value:
            self.end_value = new_val


def build_incremental_filter(
    checkpoint: Checkpoint,
    update_key: str | None,
    glob_template: str | None = None,
    partition_date: str | None = None,
) -> str:
    """Constructs SQL WHERE clause using active checkpoint state.

    Args:
        update_key: Target column name used for watermarking.
        glob_template: Path pattern for file-based filtering.
        partition_date: Specific partition date override.

    Returns:
        Formatted SQL predicate string (defaults to '1=1').
    """
    if not update_key:
        return "1=1"

    match checkpoint.type:
        case CheckpointType.UPDATE_KEY:
            return _build_update_key_filter(
                checkpoint, update_key, glob_template, partition_date
            )
        case CheckpointType.LSN_OFFSET:
            return _build_lsn_filter(checkpoint, update_key)
        case _:
            return "1=1"


def _build_lsn_filter(checkpoint: Checkpoint, update_key: str) -> str:
    """Builds predicate bounds for Log Sequence Number streams.

    Args:
        update_key: Target LSN/offset column name.

    Returns:
        SQL condition bounded by start and end LSN values.
    """
    LOG.debug(f"Generating where condition for update_key={update_key}")
    preds = []
    if checkpoint.start_value:
        preds.append(f"{update_key} > '{format_sql_value(checkpoint.start_value)}'")
    if checkpoint.end_value:
        preds.append(f"{update_key} <= '{format_sql_value(checkpoint.end_value)}'")
    return " AND ".join(preds) if preds else "1=1"


def _build_update_key_filter(
    checkpoint: Checkpoint,
    update_key: str,
    glob_template: str | None,
    partition_date: str | None,
) -> str:
    """Routes update key evaluation based on column strategy type.

    Args:
        update_key: Target filtering column name.
        glob_template: Path format for object store keys.
        partition_date: Target date string override.

    Returns:
        Target predicate string for specified update key.
    """
    match update_key:
        case "last_modified_timestamp" | "last_modified" | "mtime":
            if not checkpoint.start_value:
                return "1=1"
            try:
                ts = datetime.fromisoformat(checkpoint.start_value).timestamp()
            except ValueError:
                ts = float(checkpoint.start_value)
            return f"last_modified > {ts}"

        case "partition_date":
            return _build_partition_date_filter(
                checkpoint, update_key, glob_template, partition_date
            )

        case _:
            return _build_default_range_filter(checkpoint, update_key, partition_date)


def _build_partition_date_filter(
    checkpoint: Checkpoint,
    update_key: str,
    glob_template: str | None,
    partition_date: str | None,
) -> str:
    """Generates IN list or file path predicates for date targets.

    Args:
        update_key: Partition date column name.
        glob_template: String template for file key matching.
        partition_date: Explicit single date filter override.

    Returns:
        SQL expression filtering target dates or file paths.
    """
    dates = _resolve_target_dates(checkpoint, partition_date)
    if not dates:
        return "1=1"

    if glob_template:
        raw_glob = glob_template.replace("{{", "{").replace("}}", "}")
        sql_conditions: list[str] = []

        for d in dates:
            dt = datetime.strptime(d, DATE_FORMAT)
            tokens = {
                "partition_date": d,
                "year": dt.strftime("%Y"),
                "YYYY": dt.strftime("%Y"),
                "month": dt.strftime("%m"),
                "MM": dt.strftime("%m"),
                "day": dt.strftime("%d"),
                "DD": dt.strftime("%d"),
                "YYYYMMDD": dt.strftime("%Y%m%d"),
            }
            pattern = raw_glob.format(**tokens).replace("*", "%")
            sql_conditions.append(f"path LIKE '%{pattern}'")

        return f"({' OR '.join(sql_conditions)})"

    formatted_dates = ", ".join(f"'{d}'" for d in dates)
    return f"{update_key} IN ({formatted_dates})"


def _resolve_target_dates(
    checkpoint: Checkpoint, partition_date: str | None
) -> list[str]:
    """Resolves active date range between start and end watermarks.

    Args:
        partition_date: Direct partition date string override.

    Returns:
        List of formatted YYYY-MM-DD date strings.
    """
    if partition_date:
        return [partition_date]
    if not checkpoint.end_value:
        return []

    end_dt = datetime.strptime(checkpoint.end_value, DATE_FORMAT)
    if not checkpoint.start_value:
        return [end_dt.strftime(DATE_FORMAT)]

    start_dt = datetime.strptime(checkpoint.start_value, DATE_FORMAT)
    days = (end_dt - start_dt).days
    return [
        (start_dt + timedelta(days=i + 1)).strftime(DATE_FORMAT) for i in range(days)
    ]


def _build_default_range_filter(
    checkpoint: Checkpoint, update_key: str, partition_date: str | None
) -> str:
    """Generates bounded comparison range for primary key/timestamps.

    Args:
        update_key: Column name used for continuous ranges.
        partition_date: Upper boundary override string.

    Returns:
        SQL expression containing lower and upper bounds.
    """
    preds = []
    if checkpoint.start_value:
        preds.append(f"{update_key} >= '{format_sql_value(checkpoint.start_value)}'")

    if effective_end := (partition_date or checkpoint.end_value):
        try:
            end_dt = datetime.strptime(effective_end, DATE_FORMAT)
            next_day = (end_dt + timedelta(days=1)).strftime(TIMESTAMP_FORMAT)
            preds.append(f"{update_key} < '{next_day}'")
        except ValueError:
            preds.append(f"{update_key} <= '{format_sql_value(effective_end)}'")

    return " AND ".join(preds) if preds else "1=1"
