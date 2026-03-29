-- DDL for orders dataset in Silver layer
CREATE TABLE IF NOT EXISTS test.orders (
    order_id Int64,
    customer_id Int64,
    order_date Date,
    amount Float64,
    status String,
    updated_at DateTime64(3, 'UTC'),
    -- Metadata Columns
    _created_at_ts DateTime64(3, 'UTC') DEFAULT now(),
    _partition String,
    _source String,
    _run_id String,
) ENGINE = MergeTree()
ORDER BY (_partition, order_date, order_id);
