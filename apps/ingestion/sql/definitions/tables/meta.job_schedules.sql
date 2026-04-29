
-- This table defines the job schedules for data ingestion tasks. 
-- It includes details about the job, its scheduling, and execution parameters.
-- Each row is an update to a job's schedule, allowing us to maintain a history of changes over time.

DROP TABLE IF EXISTS META.JOB_SCHEDULES;
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
    -- , PARTITION_DATE        Date -- Added to support partitioning/ordering
    -- , NEXT_RUN_TS_LC           DateTime64(3)
    -- , PREV_RUN_TS_LC           DateTime64(3)
    , LAST_UPDATED_AT_TS_LC    DateTime64(3) DEFAULT now64(3, 'Asia/Singapore')
    , CREATED_AT_TS_LC         DateTime64(3) DEFAULT now64(3, 'Asia/Singapore')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(CREATED_AT_TS_LC)
ORDER BY (JOB_ID, DATASET_ID, LAST_UPDATED_AT_TS_LC)
