DROP TABLE IF EXISTS META.EXECUTION_LOG;
CREATE TABLE META.EXECUTION_LOG (
    RUN_ID                  Nullable(FixedString(22))
    , JOB_ID                LowCardinality(String)
    , DATASET_ID            LowCardinality(String)
    , RUN_DATE              Nullable(Date)
    , SCHEDULED_TIMESTAMP   DateTime64(3)
    , START_TIMESTAMP       Nullable(DateTime64(3))
    , END_TIMESTAMP         Nullable(DateTime64(3))
    , LAST_UPDATED_AT_TS    DateTime64(3) DEFAULT now64()
    , JOB_STATUS            LowCardinality(String)
    , CURRENT_STEP          LowCardinality(Nullable(String))
    , JOB_BITMASK           UInt16 DEFAULT 0
    , WATCH_FILE_PATH       Nullable(String)
    , RUNTIME_OVERRIDES     Nullable(String) -- JSON representation
    , RETRY_ATTEMPTS        UInt8 DEFAULT 0
    , SOURCE_ROW_COUNT      Nullable(UInt64) 
    , FINAL_ROW_COUNT       Nullable(UInt64) 
    , FINAL_MANIFEST        Nullable(String) -- JSON representation
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(SCHEDULED_TIMESTAMP)
ORDER BY (JOB_ID, DATASET_ID,LAST_UPDATED_AT_TS)
TTL SCHEDULED_TIMESTAMP + INTERVAL 12 MONTH;
