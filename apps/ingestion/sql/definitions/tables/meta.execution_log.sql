CREATE TABLE IF NOT EXISTS META.EXECUTION_LOG (
    RUN_ID                  CHAR(22)
    , JOB_ID                LowCardinality(String)
    , DATASET_ID            LowCardinality(String)
    , RUN_DATE              DATE
    , SCHEDULED_TIMESTAMP   DateTime64(3, 'UTC')
    , START_TIMESTAMP       DateTime64(3, 'UTC')
    , END_TIMESTAMP         DateTime64(3, 'UTC')
    , LAST_UPDATED_AT_TS    DateTime64(3, 'UTC') DEFAULT now64()
    , JOB_STATUS            LowCardinality(String)
    , CURRENT_STEP          LowCardinality(String)
    , JOB_BITMASK           UInt16 DEFAULT 0
    , WATCH_FILE_PATH       String
    , RUNTIME_OVERRIDES     String -- JSON representation
    , RETRY_ATTEMPTS        UInt8 DEFAULT 0
    , SOURCE_ROW_COUNT      UInt64
    , FINAL_ROW_COUNT       UInt64
    , FINAL_MANIFEST        String -- JSON representation
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(SCHEDULED_TIMESTAMP)
ORDER BY (JOB_ID, RUN_DATE, LAST_UPDATED_AT_TS)
TTL SCHEDULED_TIMESTAMP + INTERVAL 12 MONTH;



