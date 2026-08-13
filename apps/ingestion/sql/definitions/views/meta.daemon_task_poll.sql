DROP VIEW IF EXISTS META.DAEMON_TASK_POLL;
CREATE VIEW META.DAEMON_TASK_POLL AS
WITH dates AS (
    SELECT
        8 AS OFFSET_HOURS,
        now64(3) + INTERVAL OFFSET_HOURS HOUR AS NOW_TS_LC
),

-- 1. Filter active in-flight tasks and pending tasks up to the lookahead window
filtered_tasks AS (
    SELECT *
    FROM META.CURRENT_EXECUTION
    WHERE RUN_ID IS NOT NULL
    AND (
        -- Always monitor in-flight tasks
        JOB_STATUS IN ('PROVISIONED', 'WAITING', 'DISPATCHED', 'RUNNING')
        OR
        -- Fetch pending tasks up to 15 minutes in advance (no lower bound)
        (
            JOB_STATUS IN ('PENDING')
            AND SCHEDULED_TIMESTAMP_LC <= (SELECT NOW_TS_LC FROM dates) + INTERVAL 15 MINUTE
        )
    )
),

-- 2. Tag tasks as upcoming vs. due/overdue
task_scored AS (
    SELECT
        t.*,
        (t.SCHEDULED_TIMESTAMP_LC > (SELECT NOW_TS_LC FROM dates)) AS is_upcoming
    FROM filtered_tasks t
),

-- 3. Calculate execution ranks per job policy
task_ranked AS (
    SELECT
        t.*,

        -- Snapshot Policy (Due/Overdue Stream):
        -- Pick the most recent due/overdue task (<= NOW)
        ROW_NUMBER() OVER (
            PARTITION BY JOB_ID, DATASET_ID, PARTITION_DATE, is_upcoming
            ORDER BY t.SCHEDULED_TIMESTAMP_LC DESC
        ) AS rn_snapshot_due,

        -- Snapshot Policy (Upcoming Stream):
        -- Pick the soonest upcoming tasks (> NOW)
        ROW_NUMBER() OVER (
            PARTITION BY JOB_ID, DATASET_ID, PARTITION_DATE, is_upcoming
            ORDER BY t.SCHEDULED_TIMESTAMP_LC ASC
        ) AS rn_snapshot_upcoming,

        -- Incremental Policy: FIFO catch-up (oldest pending interval first)
        ROW_NUMBER() OVER (
            PARTITION BY JOB_ID, DATASET_ID, PARTITION_DATE
            ORDER BY t.SCHEDULED_TIMESTAMP_LC ASC
        ) AS rn_incremental
    FROM task_scored t
)

-- 4. Final selection
SELECT * EXCEPT (is_upcoming, rn_snapshot_due, rn_snapshot_upcoming, rn_incremental)
FROM task_ranked
WHERE JOB_STATUS IN ('PROVISIONED', 'WAITING', 'DISPATCHED', 'RUNNING')
    -- Snapshot Policy:
    -- 1) Pick the single latest due/overdue snapshot task
    -- 2) PLUS pick top 2 upcoming snapshot tasks as a buffer
    OR (
        COALESCE(IS_SNAPSHOT, FALSE) = TRUE
        AND (
            (NOT is_upcoming AND rn_snapshot_due = 1)
            OR (is_upcoming AND rn_snapshot_upcoming <= 2)
        )
    )
    -- Incremental Policy: Pick top 2 oldest pending intervals to process sequentially
    OR (COALESCE(IS_SNAPSHOT, FALSE) = FALSE AND rn_incremental <= 2);
