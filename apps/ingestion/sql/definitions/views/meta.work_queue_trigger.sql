-- This Materialized View acts as the "Cron Engine"
-- It runs every minute and pushes 'DUE' jobs into the execution log.
CREATE MATERIALIZED VIEW IF NOT EXISTS META.WORK_QUEUE_TRIGGER_MV
REFRESH EVERY 1 MINUTE
TO META.EXECUTION_LOG
AS
SELECT
    -- Generate a unique 22-char ID similar to Python's nanoid
    NULL AS RUN_ID
    , JOB_ID
    , DATASET_ID
    , NULL AS RUN_DATE
    , NEXT_RUN_TS AS SCHEDULED_TIMESTAMP
    , 'PENDING' AS JOB_STATUS
    , NULL AS CURRENT_STEP
    , 0 AS JOB_BITMASK
FROM META.CURRENT_SCHEDULES
WHERE IS_DUE = 1;