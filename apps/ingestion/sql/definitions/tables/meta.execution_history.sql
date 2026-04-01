CREATE TABLE IF NOT EXISTS META.EXECUTION_HISTORY (
    RUN_ID                  CHAR(22)
    , JOB_ID                LowCardinality(String)
    , DATASET_ID            LowCardinality(String)
    , RUN_DATE              Date
    , START_TIMESTAMP       DateTime64(3, 'UTC')
    , END_TIMESTAMP         DateTime64(3, 'UTC')
    , JOB_STATUS            LowCardinality(String)
    , SOURCE_ROW_COUNT      UInt64
    , FINAL_ROW_COUNT       UInt64
    , FINAL_MANIFEST        String
) 
ENGINE = MergeTree()
ORDER BY (JOB_ID, RUN_DATE, RUN_ID);