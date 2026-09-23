from collections.abc import Sequence
from enum import StrEnum

from msgspec import Struct

from src.core.stages.contracts.payload import BasePayload
from src.core.stages.models import WriteMode


class PublishPayload(BasePayload, kw_only=True, tag="publish"):
    """Publish stage results."""

    final_path: str
    rows_processed: int
    start_time: str
    # stage: Stage = Stage.PUBLISH


class DeltaMergeType(StrEnum):
    DELETE_INSERT = "delete_insert"
    UPDATE_INSERT = "update_insert"
    UPDATE = "update"


class PublishPolicy(Struct):
    merge_ops: Sequence[str]
    requires_pks: bool = False
    requires_update_key: bool = False


PUBLISH_POLICIES: dict[tuple[WriteMode, DeltaMergeType | None], PublishPolicy] = {
    # Non-DELTA Modes (DeltaMergeType is None)
    (WriteMode.FULL_REFRESH, None): PublishPolicy(merge_ops=("truncate", "insert")),
    (WriteMode.SNAPSHOT, None): PublishPolicy(
        merge_ops=("delete", "insert"), requires_update_key=True
    ),
    (WriteMode.CDC, None): PublishPolicy(
        merge_ops=("update", "insert"),
        requires_pks=True,
    ),
    # DELTA Modes (Resolved dynamically via DeltaMergeType)
    (WriteMode.DELTA, DeltaMergeType.DELETE_INSERT): PublishPolicy(
        merge_ops=("delete", "insert"), requires_update_key=True
    ),
    (WriteMode.DELTA, DeltaMergeType.UPDATE_INSERT): PublishPolicy(
        merge_ops=("update", "insert"), requires_pks=True
    ),
    (WriteMode.DELTA, DeltaMergeType.UPDATE): PublishPolicy(
        merge_ops=("update",), requires_pks=True
    ),
}
