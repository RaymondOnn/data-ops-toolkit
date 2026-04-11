-- Seed data for testing the Always-On Orchestrator
INSERT INTO META.JOB_SCHEDULES (
    JOB_ID
    , DATASET_ID
    , DATASET_NAME
    , CRON_EXPR
    , IS_ACTIVE
    , IS_SNAPSHOT
    , PRIORITY
    , TIMEOUT_SECS
    , CONCURRENCY_LIMIT
    , MISFIRE_GRACE_SECS
    , WATCH_FILE_PATH
    -- , PARTITION_DATE
    -- , NEXT_RUN_TS
    -- , PREV_RUN_TS
    , LAST_UPDATED_AT_TS
    , CREATED_AT_TS
) VALUES (
    'test_job', 'orders', 'Customer Orders', '* * * * *', TRUE, TRUE,
    1, 3600, 1, 300, NULL, 
    -- today(), now64(), now64(), 
    now64(3), now64(3)
);
