
-- For high level metadata about validation runs
CREATE TABLE IF NOT EXISTS META.VALIDATION_RUNS (
    RUN_ID              String PRIMARY KEY,
    , TRIGGERED_BY      String,
    , LOAD_MODE         LowCardinatilty(String)
    . START_TIMESTAMP   DateTime,
    . END_TIMESTAMP     DateTime,
    . STATUS            LowCardinatilty(String)
)
ENGINE = MergeTree()
ORDER BY (STATUS)


CREATE TABLE IF NOT EXISTS META.VALIDATION_RESULTS (
    ID              String PRIMARY KEY,
    RUN_ID          String,
    TABLE_NAME      String,
    STATUS          LowCardinatilty(String)
)

CREATE TABLE IF NOT EXISTS META.VALIDATION_ERRORS (
    RUN_ID          String,
    RECORD_REF      String,
    TABLE_NAME      String,
    CHECK_NAME      String,
    PAYLOAD   String -- JSON
)

CREATE VIEW IF NOT EXISTS META.MISMATCHES (
    RUN_ID          String,
    RECORD_REF      String,
    ISSUE_TYPE      LowCardinatilty(String),
    SUB_TYPE        LowCardinatilty(String),
    CULPRIT_COLUMNS Array(String),
    SOURCE_SNAPSHOT String, -- JSON
    SINK_SNAPSHOT   String, -- JSON
    METADATA        String, -- JSON: {source_ds: str, sink_ds: str, check_type: str, check_details: dict}
    LOCATORS        String  -- JSON: {table: str, filter_sql: str}
    MESSAGE         String
)
ENGINE = MergeTree()
ORDER BY (RUN_ID)


