DROP VIEW IF EXISTS META.CURRENT_SCHEDULES;
CREATE VIEW META.CURRENT_SCHEDULES AS
WITH dates AS (
    SELECT
        8 AS OFFSET_HOURS
        , now64(3) + INTERVAL OFFSET_HOURS HOUR AS NOW_TS_LC
        , toDate(NOW_TS_LC) AS TODAY_LC
        , toStartOfDay(NOW_TS_LC) AS TODAY_START_LC
)
, latest_schedules AS (
    -- Deduplicate base schedules without using FINAL
    SELECT *
    FROM (
        SELECT * FROM META.JOB_SCHEDULES
        WHERE IS_ACTIVE = 1 -- Filter for active versions BEFORE picking the latest one
        ORDER BY LAST_UPDATED_AT_TS_LC DESC
        LIMIT 1 BY JOB_ID, DATASET_ID
    )
)
, time_spine AS (
    -- OPTIMIZATION: Generate a window covering the last 24 hours to ensure all daily jobs appear.
    -- 1441 minutes total (24 hours + 1 min overlap).
    SELECT
        toDateTime64((SELECT TODAY_START_LC FROM dates) + INTERVAL number MINUTE, 3) as tick
    FROM numbers(1441)
)
, expanded_occurrences AS (
    -- Find expected runs within the optimized window
    SELECT *
    FROM (
        SELECT
            JS.JOB_ID
            , JS.DATASET_ID
            , T.tick as PLANNED_TS_LC
        FROM latest_schedules JS
        CROSS JOIN time_spine T
        WHERE TRUE
            -- toString to remove timezone metadata
            AND (toString(toDateTime(cron_next(
                JS.CRON_EXPR,
                T.tick - INTERVAL 1 SECOND
            ))) = toString(toDateTime(T.tick)))
    )
    WHERE toDate(PLANNED_TS_LC) = (SELECT TODAY_LC FROM dates)
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
        -- 4. Higher SCHEDULED_TIMESTAMP_LC wins.
        -- 5. Higher LAST_UPDATED_AT_TS_LC wins (tie-breaker for reruns).
        , argMax(JOB_STATUS, (IS_SCHEDULED, SCHEDULED_TIMESTAMP_LC, isNotNull(PARTITION_DATE), PARTITION_DATE,  LAST_UPDATED_AT_TS_LC)) as LATEST_STATUS
        , argMax(SCHEDULED_TIMESTAMP_LC, (IS_SCHEDULED, SCHEDULED_TIMESTAMP_LC, isNotNull(PARTITION_DATE), PARTITION_DATE, LAST_UPDATED_AT_TS_LC)) as LAST_TRIGGER_TS
        , argMax(LAST_UPDATED_AT_TS_LC, (IS_SCHEDULED, SCHEDULED_TIMESTAMP_LC, isNotNull(PARTITION_DATE), PARTITION_DATE, LAST_UPDATED_AT_TS_LC)) as LAST_UPDATE_TS
        , argMax(START_TIMESTAMP_LC, (IS_SCHEDULED, SCHEDULED_TIMESTAMP_LC, isNotNull(PARTITION_DATE), PARTITION_DATE, LAST_UPDATED_AT_TS_LC)) as PREV_RUN_TS_LC
    FROM META.EXECUTION_LOG
    GROUP BY JOB_ID, DATASET_ID
)
SELECT
    S.JOB_ID AS JOB_ID
    , S.DATASET_ID AS DATASET_ID
    , S.DATASET_NAME AS DATASET_NAME
    , S.CRON_EXPR AS CRON_EXPR
    , S.IS_ACTIVE AS IS_ACTIVE
    , S.IS_SNAPSHOT AS IS_SNAPSHOT
    , S.PRIORITY AS PRIORITY
    , L.LATEST_STATUS AS LATEST_STATUS
    -- The grain is now the PLANNED_TS_LC (Specific occurrence)
    , E.PLANNED_TS_LC AS NEXT_RUN_TS_LC
    -- Logic: It is DUE if it hasn't run yet AND (it's time to run OR it's a retry)
    , (
        S.IS_ACTIVE = 1
        AND (
            -- CASE A: It's a scheduled slot for today that hasn't been logged yet
            -- We check if this specific slot is already in EXECUTION_LOG elsewhere
            -- or if it is simply time to fire
            -- REFACTOR: Look ahead 60 minutes to proactively seed the Execution Log
            -- and generate non-null RUN_IDs before the application polls.
            E.PLANNED_TS_LC <= addMinutes(NOW_TS_LC, 60)
        )
      ) AS IS_DUE
    , (S.IS_ACTIVE = 1 AND toDate(E.PLANNED_TS_LC) = NOW_TS_LC) AS IS_SCHEDULED_TODAY
    -- Dynamic lookup from Execution Log
    , L.PREV_RUN_TS_LC AS PREV_RUN_TS_LC
    , S.LAST_UPDATED_AT_TS_LC AS LAST_UPDATED_AT_TS_LC
    , LOG.RUN_ID AS LOG_RUN_ID
    , (SELECT NOW_TS_LC FROM dates) as NOW_TS_LC
FROM latest_schedules S
JOIN expanded_occurrences E ON S.JOB_ID = E.JOB_ID AND S.DATASET_ID = E.DATASET_ID
LEFT JOIN latest_runs L ON S.JOB_ID = L.JOB_ID AND S.DATASET_ID = L.DATASET_ID
-- Ensure we don't trigger slots that are already registered in the log
LEFT ANY JOIN META.EXECUTION_LOG LOG
    ON S.JOB_ID = LOG.JOB_ID
    AND S.DATASET_ID = LOG.DATASET_ID
    AND E.PLANNED_TS_LC = LOG.SCHEDULED_TIMESTAMP_LC
WHERE LOG.RUN_ID='' -- Logic: Only show slots that have no corresponding entry in Execution Log
ORDER BY NEXT_RUN_TS_LC;


-- For check if all records are inserted exactly once
-- SELECT
--     JOB_ID,
--     DATASET_ID,
--     SCHEDULED_TIMESTAMP_LC,
--     count() AS insert_count,
--     uniqExact(RUN_ID) AS unique_run_ids,
--     groupArray(LAST_UPDATED_AT_TS_LC) AS update_timestamps
-- FROM META.EXECUTION_LOG
-- GROUP BY
--     JOB_ID,
--     DATASET_ID,
--     SCHEDULED_TIMESTAMP_LC
-- HAVING insert_count > 1
-- ORDER BY SCHEDULED_TIMESTAMP_LC DESC;
