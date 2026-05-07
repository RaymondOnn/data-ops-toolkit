DROP VIEW IF EXISTS META.APPEND_LOG_TRIGGER;
CREATE MATERIALIZED VIEW META.APPEND_LOG_TRIGGER
REFRESH EVERY 1 MINUTE
APPEND TO META.EXECUTION_LOG
AS
WITH dates AS (
    SELECT now64(3) + INTERVAL 8 HOUR AS NOW_TS_LC
) 
SELECT
    concat(
        formatDateTime(CS.NEXT_RUN_TS_LC, '%Y%m%d-%H%i%s'), 
        '-', 
        substring(lower(hex(MD5(concat(CS.JOB_ID, CS.DATASET_ID, toString(CS.NEXT_RUN_TS_LC))))), 1, 8)
    ) AS RUN_ID
    , CS.JOB_ID AS JOB_ID
    , CS.DATASET_ID AS DATASET_ID
    , cast(NULL, 'Nullable(Date)') AS PARTITION_DATE
    , CS.NEXT_RUN_TS_LC AS SCHEDULED_TIMESTAMP_LC
    , cast(NULL, 'Nullable(DateTime64(3))') AS START_TIMESTAMP_LC
    , cast(NULL, 'Nullable(DateTime64(3))') AS END_TIMESTAMP_LC
    , (SELECT NOW_TS_LC FROM dates) AS LAST_UPDATED_AT_TS_LC
    -- 3. Added S. and CS. prefixes inside the IF logic to avoid ambiguity
    , cast(
        if(S.IS_SNAPSHOT = 1 AND (SELECT NOW_TS_LC FROM dates) > cron_next(if(empty(S.CRON_EXPR), '0 0 * * *', S.CRON_EXPR), CS.NEXT_RUN_TS_LC), 
            'EXPIRED', 
            'PENDING'
        ), 'LowCardinality(String)'
    ) AS JOB_STATUS
    , cast(NULL, 'LowCardinality(Nullable(String))') AS CURRENT_STAGE
    , cast(0, 'UInt16') AS JOB_BITMASK 
    , cast(1, 'UInt8') AS IS_SCHEDULED
    , CS.WATCH_FILE_PATH AS WATCH_FILE_PATH
    , cast(NULL, 'Nullable(String)') AS RUNTIME_OVERRIDES
    , cast(0, 'UInt8') AS RETRY_ATTEMPTS
    , cast(NULL, 'Nullable(UInt64)') AS SOURCE_ROW_COUNT
    , cast(NULL, 'Nullable(UInt64)') AS FINAL_ROW_COUNT
    , cast(NULL, 'Nullable(String)') AS FINAL_MANIFEST
    , cast(NULL, 'Nullable(String)') AS REMARKS
FROM META.CURRENT_SCHEDULES AS CS
LEFT ANY JOIN (
    SELECT 
        JOB_ID
        , DATASET_ID
        , IS_SNAPSHOT
        , CRON_EXPR 
    FROM META.JOB_SCHEDULES 
    LIMIT 1 BY JOB_ID, DATASET_ID
) AS S ON CS.JOB_ID = S.JOB_ID AND CS.DATASET_ID = S.DATASET_ID
WHERE CS.IS_DUE = 1;