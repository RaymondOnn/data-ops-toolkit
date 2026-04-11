DROP VIEW IF EXISTS META.CURRENT_SCHEDULES;
CREATE VIEW META.CURRENT_SCHEDULES AS
WITH latest_schedules AS (
    -- Deduplicate base schedules without using FINAL
    SELECT *
    FROM (
        SELECT * FROM META.JOB_SCHEDULES
        WHERE IS_ACTIVE = 1 -- Filter for active versions BEFORE picking the latest one
        ORDER BY LAST_UPDATED_AT_TS DESC
        LIMIT 1 BY JOB_ID, DATASET_ID
    ) 
)
, latest_runs AS (
    -- Get the last start time for every unique dataset
    SELECT 
        JOB_ID
        , DATASET_ID
        -- Priority Logic for "Latest": 
        -- 1. Scheduled runs (IS_SCHEDULED=1) win over manual ones (IS_SCHEDULED=0).
        -- 2. Records with a PARTITION_DATE win over NULL (Queued) ones.
        -- 3. Higher PARTITION_DATE wins.
        -- 4. Higher SCHEDULED_TIMESTAMP wins.
        -- 5. Higher LAST_UPDATED_AT_TS wins (tie-breaker for reruns).
        , argMax(JOB_STATUS, (IS_SCHEDULED, SCHEDULED_TIMESTAMP, isNotNull(PARTITION_DATE), PARTITION_DATE,  LAST_UPDATED_AT_TS)) as LATEST_STATUS
        , argMax(SCHEDULED_TIMESTAMP, (IS_SCHEDULED, SCHEDULED_TIMESTAMP, isNotNull(PARTITION_DATE), PARTITION_DATE, LAST_UPDATED_AT_TS)) as LAST_TRIGGER_TS
        , argMax(LAST_UPDATED_AT_TS, (IS_SCHEDULED, SCHEDULED_TIMESTAMP, isNotNull(PARTITION_DATE), PARTITION_DATE, LAST_UPDATED_AT_TS)) as LAST_UPDATE_TS
        , argMax(START_TIMESTAMP, (IS_SCHEDULED, SCHEDULED_TIMESTAMP, isNotNull(PARTITION_DATE), PARTITION_DATE, LAST_UPDATED_AT_TS)) as PREV_RUN_TS
    FROM META.EXECUTION_LOG
    GROUP BY JOB_ID, DATASET_ID
)
, time_spine AS (
    -- OPTIMIZATION: Generate a window covering the last 24 hours to ensure all daily jobs appear.
    -- 1441 minutes total (24 hours + 1 min overlap).
    SELECT 
        toDateTime64(toStartOfMinute(now64(3, 'Asia/Singapore')) - INTERVAL 24 HOUR + INTERVAL number MINUTE, 3) as tick
    FROM numbers(1441)
)
, expanded_occurrences AS (
    -- Find expected runs within the optimized window
    SELECT 
        JS.JOB_ID
        , JS.DATASET_ID
        , T.tick as PLANNED_TS
    FROM latest_schedules JS
    CROSS JOIN time_spine T
    WHERE TRUE
        -- toString to remove timezone metadata
        AND (toString(toDateTime(cron_next(
            JS.CRON_EXPR, 
            T.tick - INTERVAL 1 SECOND
        ))) = toString(toDateTime(T.tick)))
)
SELECT
    S.JOB_ID AS JOB_ID
    , S.DATASET_ID AS DATASET_ID
    , S.DATASET_NAME AS DATASET_NAME
    , S.CRON_EXPR AS CRON_EXPR
    , S.IS_ACTIVE AS IS_ACTIVE
    , S.IS_SNAPSHOT AS IS_SNAPSHOT
    , S.PRIORITY AS PRIORITY
    , S.WATCH_FILE_PATH AS WATCH_FILE_PATH
    , L.LATEST_STATUS AS LATEST_STATUS
    -- The grain is now the PLANNED_TS (Specific occurrence)
    , E.PLANNED_TS AS NEXT_RUN_TS
    -- Logic: It is DUE if it hasn't run yet AND (it's time to run OR it's a retry)
    , (
        S.IS_ACTIVE = 1 
        AND (
            -- CASE A: It's a scheduled slot for today that hasn't been logged yet
            -- We check if this specific slot is already in EXECUTION_LOG elsewhere 
            -- or if it is simply time to fire
            E.PLANNED_TS <= addSeconds(now64(3, 'Asia/Singapore'), 120)
            OR 
            -- CASE B: Maintenance/Retry logic
            (
                L.LATEST_STATUS = 'DEFERRED' 
                AND now64(3, 'Asia/Singapore') >= addHours(L.LAST_UPDATE_TS, 2)
            )
        )
      ) AS IS_DUE
    , (S.IS_ACTIVE = 1 AND toDate(E.PLANNED_TS) = toDate(now64(3, 'Asia/Singapore'))) AS IS_SCHEDULED_TODAY
    -- Dynamic lookup from Execution Log
    , L.PREV_RUN_TS AS PREV_RUN_TS
    , S.LAST_UPDATED_AT_TS AS LAST_UPDATED_AT_TS
    , LOG.ROW_ID AS LOG_IDX
    , now64(3, 'Asia/Singapore') as NOW_TS_UTC
FROM latest_schedules S
JOIN expanded_occurrences E ON S.JOB_ID = E.JOB_ID AND S.DATASET_ID = E.DATASET_ID
LEFT JOIN latest_runs L ON S.JOB_ID = L.JOB_ID AND S.DATASET_ID = L.DATASET_ID
-- Ensure we don't trigger slots that are already registered in the log
LEFT ANY JOIN META.EXECUTION_LOG LOG 
    ON S.JOB_ID = LOG.JOB_ID 
    AND S.DATASET_ID = LOG.DATASET_ID 
    AND E.PLANNED_TS = LOG.SCHEDULED_TIMESTAMP
WHERE LOG.ROW_ID = 0 -- Check for 0 (default UInt64) instead of NULL to identify missing runs
ORDER BY NEXT_RUN_TS; 