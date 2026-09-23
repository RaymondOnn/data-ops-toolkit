DROP TABLE IF EXISTS META.SCHEMA_EVOLUTION_LOG;
CREATE TABLE META.SCHEMA_EVOLUTION_LOG (
    RUN_ID          FixedString(26)        -- Ties back to pipeline/task execution
    , TARGET_TABLE    String
    , ACTION          LowCardinality(String) -- e.g., 'ADD_COLUMN', 'MODIFY_COLUMN'
    , COLUMN_NAME     String
    , DATA_TYPE       LowCardinality(String)
    , SCHEMA          String
    , EXECUTED_AT_LC  DateTime64(3, 'UTC') DEFAULT now64(3)
    , EXECUTED_BY     String DEFAULT 'app_schema_engine'
    , LAST_UPDATED_AT_TS_LC     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (TARGET_TABLE, EXECUTED_AT_LC)
