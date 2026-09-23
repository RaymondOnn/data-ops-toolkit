from enum import StrEnum


class WriteMode(StrEnum):
    DELTA = "delta"
    SNAPSHOT = "snapshot"
    FULL_REFRESH = "full_refresh"
    CDC = "cdc"
    # BACKFILL = "backfill". #?
