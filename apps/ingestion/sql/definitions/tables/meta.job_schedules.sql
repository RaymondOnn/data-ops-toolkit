CREATE DATABASE IF NOT EXISTS META;

CREATE TABLE IF NOT EXISTS META.JOB_SCHEDULES (
    JOB_ID                  LowCardinality(String)
    , DATASET_ID            LowCardinality(String)
    , DATASET_NAME          String
    , CRON_EXPR             String -- Trigger timing
    , IS_ACTIVE             Bool -- Master Kill Switch
    , IS_SNAPSHOT           Bool
    , PRIORITY              UInt8 -- Worker Allocation Priority
    , TIMEOUT_SECS          UInt32 -- Max Execution Time
    , CONCURRENCY_LIMIT     UInt16 -- Max Concurrent Tasks
    , MISFIRE_GRACE_SECS    Int32 -- Run if X Seconds late (0=Never, -1=Always)
    , WATCH_FILE_PATH       String -- For file monitoring
    , RUN_DATE              Date -- Added to support partitioning/ordering
    , NEXT_RUN_TS           DateTime64(3, 'UTC')
    , PREV_RUN_TS           DateTime64(3, 'UTC')
    , LAST_UPDATED_AT_TS    DateTime64(3, 'UTC') DEFAULT now64()
    , CREATED_AT_TS         DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(CREATED_AT_TS)
ORDER BY (JOB_ID, DATASET_ID, LAST_UPDATED_AT_TS)
