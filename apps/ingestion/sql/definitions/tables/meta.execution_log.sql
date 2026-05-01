DROP TABLE IF EXISTS META.EXECUTION_LOG;
CREATE TABLE META.EXECUTION_LOG (
    RUN_ID                  FixedString(26)
    , JOB_ID                LowCardinality(String)
    , DATASET_ID            LowCardinality(String)
    , PARTITION_DATE        Nullable(Date)
    , SCHEDULED_TIMESTAMP_LC   DateTime64(3, 'UTC')
    , START_TIMESTAMP_LC       Nullable(DateTime64(3, 'UTC'))
    , END_TIMESTAMP_LC         Nullable(DateTime64(3, 'UTC'))
    , LAST_UPDATED_AT_TS_LC    DateTime64(3, 'UTC') DEFAULT now64(3)
    , JOB_STATUS            LowCardinality(String)
    , CURRENT_STEP          LowCardinality(Nullable(String))
    , JOB_BITMASK           UInt16 DEFAULT 0
    , IS_SCHEDULED          UInt8 DEFAULT 0 -- 1 = Scheduled, 0 = Manual/Ad-hoc
    , WATCH_FILE_PATH       Nullable(String)
    , RUNTIME_OVERRIDES     Nullable(String) -- JSON representation
    , RETRY_ATTEMPTS        UInt8 DEFAULT 0
    , SOURCE_ROW_COUNT      Nullable(UInt64) 
    , FINAL_ROW_COUNT       Nullable(UInt64) 
    , FINAL_MANIFEST        Nullable(String) -- JSON representation
    , REMARKS               Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(LAST_UPDATED_AT_TS_LC)
ORDER BY (JOB_ID, DATASET_ID, RUN_ID, LAST_UPDATED_AT_TS_LC)
TTL LAST_UPDATED_AT_TS_LC + INTERVAL 12 MONTH;
