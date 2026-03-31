CREATE TABLE IF NOT EXISTS META.JOB_SCHEDULES (
    JOB_ID                  VARCHAR(50) PRIMARY KEY
    , CRON_EXPR             CHAR(6) -- Trigger timing
    , IS_ACTIVE             BOOLEAN -- Master Kill Switch
    , IS_SNAPSHOT           BOOLEAN
    , PRIORITY              INTEGER -- Worker Allocation Priority
    , TIMEOUT_SECS          INTEGER -- Max Execution Time
    , CONCURRENCY_LIMIT     INTEGER -- Max Concurrent Tasks
    , MISFIRE_GRACE_SECS    INTEGER -- Run if X Seconds late (0=Never, -1=Always)
    , WATCH_FILE_PATH       VARCHAR(1000) -- For file monitoring
    , NEXT_RUN_TS           TIMESTAMP_LTZ
    . PREV_RUN_TS           TIMESTAMP_LTZ
    , LAST_UPDATED_AT_TS    TIMESTAMP_LTZ DEFAULT NOW()
    , CREATED_AT_TS         TIMESTAMP_LTZ
)