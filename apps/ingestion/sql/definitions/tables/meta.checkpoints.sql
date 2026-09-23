DROP TABLE IF EXISTS META.CHECKPOINTS;
CREATE TABLE IF NOT EXISTS META.CHECKPOINTS (
    RUN_ID                      FixedString(26) -- Base62 encoded UUIDv4
    , JOB_ID                    LowCardinality(String)
    , DATASET_ID                LowCardinality(String)
    , PARTITION_DATE            Date -- Logical partition date
    , MODE                      LowCardinality(String) -- SNAPSHOT / INCREMENTAL / TRUNCATE / BACKFILL
    , CHECKPOINT_TYPE           LowCardinality(String) -- 'LSN_OFFSET', UPDATE_KEY, PAGE_OFFSET
    , CHECKPOINT_START_VALUE    String -- Min PK value or start timestamp
    , CHECKPOINT_END_VALUE      String -- Max PK value or current high-water mark timestamp
    , ROWS_LOADED               UInt64 DEFAULT 0
    , SOURCE                    String
    , CREATED_AT_TS_LC          DateTime64(3, 'UTC') DEFAULT now64(3)
    , LAST_UPDATED_AT_TS_LC     DateTime64(3, 'UTC') DEFAULT now64(3)
    , STATE_PAYLOAD             String -- Handles custom cursors, CDC positions, GTIDs
    , REMARKS                   Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(PARTITION_DATE)
ORDER BY (JOB_ID, DATASET_ID, PARTITION_DATE, CREATED_AT_TS_LC);

/*
LSN: { "lsn": "0/16B37A8" }
PAGE_OFFSET: { "page": 250000, "chunk_size": 100000 }
UPDATE_KEY: "2026-08-15 14:00:00.000" -> WHERE update_key > 'last_value'
*/
