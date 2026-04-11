DROP VIEW IF EXISTS META.CURRENT_EXECUTION;
CREATE VIEW META.CURRENT_EXECUTION AS 

-- 1. Get Scheduled slots that are DUE (Intent)
SELECT 
    CAST(NULL, 'Nullable(FixedString(22))') AS RUN_ID
    , JOB_ID
    , DATASET_ID
    , CAST(NULL, 'Nullable(Date)') AS PARTITION_DATE -- Blank from DB for schedules
    , NEXT_RUN_TS AS SCHEDULED_TIMESTAMP
    , CAST(NULL, 'Nullable(DateTime64(3))') AS START_TIMESTAMP
    , CAST(NULL, 'Nullable(DateTime64(3))') AS END_TIMESTAMP
    , NOW_TS_UTC AS LAST_UPDATED_AT_TS
    , CAST('PENDING', 'LowCardinality(String)') AS JOB_STATUS
    , CAST(NULL, 'LowCardinality(Nullable(String))') AS CURRENT_STEP
    , CAST(0, 'UInt16') AS JOB_BITMASK
    , CAST(1, 'UInt8') AS IS_SCHEDULED
    , CAST(NULL, 'Nullable(String)') AS RUNTIME_OVERRIDES
    , CAST(0, 'UInt8') AS RETRY_ATTEMPTS
    , 'CRON' AS TRIGGER_TYPE
    , WATCH_FILE_PATH
FROM META.CURRENT_SCHEDULES
WHERE IS_DUE = 1
UNION ALL


-- 2. Get existing runs from the log that are still in an active state (Reality)
SELECT 
    RUN_ID
    , JOB_ID
    , DATASET_ID
    , toString(PARTITION_DATE) AS PARTITION_DATE
    , SCHEDULED_TIMESTAMP
    , START_TIMESTAMP
    , END_TIMESTAMP
    , LAST_UPDATED_AT_TS
    , JOB_STATUS
    , CURRENT_STEP
    , JOB_BITMASK
    , IS_SCHEDULED
    , RUNTIME_OVERRIDES
    , RETRY_ATTEMPTS
    , 'CRON' AS TRIGGER_TYPE
    , WATCH_FILE_PATH
FROM META.EXECUTION_LOG
WHERE JOB_STATUS IN (
        'PENDING',
        'QUEUED',
        'PROVISIONING',
        'RUNNING',
        'DEFERRED',
        'HELD'
    );