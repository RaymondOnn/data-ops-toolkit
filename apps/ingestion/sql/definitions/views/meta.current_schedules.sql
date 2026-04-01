CREATE OR REPLACE VIEW META.CURRENT_SCHEDULES AS
WITH latest_runs AS (
    -- Get the last start time for every unique dataset
    SELECT 
        JOB_ID
        , DATASET_ID
        , argMax(JOB_STATUS, LAST_UPDATED_AT_TS) as LATEST_STATUS
        , max(SCHEDULED_TIMESTAMP) as LAST_TRIGGER_TS
        , max(LAST_UPDATED_AT_TS) as LAST_UPDATE_TS
        , START_TIMESTAMP as PREV_RUN_TS
    FROM META.EXECUTION_LOG
    GROUP BY JOB_ID, DATASET_ID
)
SELECT
    S.JOB_ID
    , S.DATASET_ID
    , S.DATASET_NAME
    , S.CRON_EXPR
    , S.IS_ACTIVE
    , S.IS_SNAPSHOT
    , S.PRIORITY
    , S.WATCH_FILE_PATH
    -- Sequential Next Run: Calculate next slot based on the last time we triggered
    -- This prevents skipping windows in sub-daily (e.g. 5-min) schedules.
    -- Logic 1: Sequential Next Run
    -- If we have a previous trigger, we find the next slot relative to IT.
    -- If the last trigger was 12:00, NEXT_RUN is 12:05, even if it is currently 12:30.
    , cron_next(S.CRON_EXPR, coalesce(L.LAST_TRIGGER_TS, subtractDays(now64(), 1))) AS NEXT_RUN_TS
    
    -- Logic 2: Catch-up/Due Evaluation
    , (
        S.IS_ACTIVE = 1 
        AND (
            -- CASE A: Last run finished (or never existed), and the next slot is ready (with 120s buffer)
            (
                (L.LATEST_STATUS IN ('SUCCESS', 'FAILED', 'EXPIRED', 'CANCELLED') OR L.LATEST_STATUS IS NULL)
                AND NEXT_RUN_TS <= addSeconds(now64(), 120)
            )
            OR 
            -- CASE B: Last run was DEFERRED and 2 hours have passed since the last update
            (
                L.LATEST_STATUS = 'DEFERRED' 
                AND now64() >= addHours(L.LAST_UPDATE_TS, 2)
            )
        )
      ) AS IS_DUE
    , (S.IS_ACTIVE = 1 AND toDate(NEXT_RUN_TS) = today()) AS IS_SCHEDULED_TODAY
    -- Dynamic lookup from Execution Log
    , L.PREV_RUN_TS AS PREV_RUN_TS
    , S.LAST_UPDATED_AT_TS
FROM META.JOB_SCHEDULES AS S
LEFT JOIN latest_runs AS L ON S.JOB_ID = L.JOB_ID AND S.DATASET_ID = L.DATASET_ID
ORDER BY S.LAST_UPDATED_AT_TS DESC
LIMIT 1 BY S.JOB_ID, S.DATASET_ID;