DROP TABLE IF EXISTS META.EXECUTION_HISTORY;
CREATE TABLE IF NOT EXISTS META.EXECUTION_HISTORY (
    RUN_ID                  FixedString(22) -- Base62 encoded UUIDv4
    , JOB_ID                LowCardinality(String)
    , DATASET_ID            LowCardinality(String)
    , PARTITION_DATE        Nullable(Date)
    , START_TIMESTAMP       DateTime64(3)
    , END_TIMESTAMP         DateTime64(3)
    , JOB_STATUS            LowCardinality(String)
    , SOURCE_ROW_COUNT      UInt64
    , FINAL_ROW_COUNT       UInt64
    , FINAL_MANIFEST        String
) 
ENGINE = MergeTree()
ORDER BY (JOB_ID, DATASET_ID, RUN_ID);