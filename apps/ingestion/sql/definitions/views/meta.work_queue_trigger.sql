-- This Materialized View acts as the "Cron Engine"
-- It runs every minute and pushes 'DUE' jobs into the execution log.

DROP VIEW IF EXISTS META.WORK_QUEUE_TRIGGER_MV;
CREATE MATERIALIZED VIEW META.WORK_QUEUE_TRIGGER_MV
REFRESH EVERY 1 MINUTE
TO META.EXECUTION_LOG
AS
SELECT
    -- Position 1: ROW_ID (Must match UInt64 type)
    sipHash64(toString(generateUUIDv4())) AS ROW_ID
    -- Position 2: RUN_ID
    , CAST(NULL, 'Nullable(FixedString(22))') AS RUN_ID
    , JOB_ID
    , DATASET_ID
    , CAST(NULL, 'Nullable(Date)') AS PARTITION_DATE
    , NEXT_RUN_TS AS SCHEDULED_TIMESTAMP
    , CAST(NULL, 'Nullable(DateTime64(3))') AS START_TIMESTAMP
    , CAST(NULL, 'Nullable(DateTime64(3))') AS END_TIMESTAMP
    , now64(3, 'Asia/Singapore') AS LAST_UPDATED_AT_TS
    , CAST('PENDING', 'LowCardinality(String)') AS JOB_STATUS
    , CAST(NULL, 'LowCardinality(Nullable(String))') AS CURRENT_STEP
    , CAST(0, 'UInt16') AS JOB_BITMASK 
    , CAST(1, 'UInt8') AS IS_SCHEDULED
FROM META.CURRENT_SCHEDULES
WHERE IS_DUE = 1;

-- SYSTEM RESUME REFRESHES META.WORK_QUEUE_TRIGGER_MV;