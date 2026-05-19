-- DDL for orders dataset in Silver layer
CREATE TABLE IF NOT EXISTS TEST.SIM_DATA(
    id Int64,
    event_date Date,
    user_name String,
    email String,
    amount Float64,
    status LowCardinality(String),
    updated_at DateTime64(3),
    -- Metadata Columns
    _created_at_ts DateTime64(3) DEFAULT now('Asia/Singapore'),
    _partition String,
    _source LowCardinality(String),
    _run_id String,
) ENGINE = MergeTree()
ORDER BY (_partition, event_date, id);
