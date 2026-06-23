import pathway as pw
from sail import SparkSession  # LakeSail's Python interface

# ==========================================
# 1. LISTENER (Pathway)
# ==========================================
# Ingest raw streaming data (e.g., from a local folder or port)
raw_stream = pw.io.json.read(
    "./streaming_input_dir",
    schema=pw.schema_from_dict({"event_id": str, "payload": str, "timestamp": int}),
)

# Pathway handles high-frequency buffering and exposes an Arrow stream
arrow_stream = pw.debug.table_to_arrow_stream(raw_stream)


# ==========================================
# 2. PRE-PROCESSOR (LakeSail)
# ==========================================
# Initialize LakeSail locally with a local Iceberg file catalog
spark = (
    SparkSession.builder.appName("EmbeddedLakehouse")
    .config("spark.sql.catalog.local_iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local_iceberg.type", "hadoop")
    .config("spark.sql.catalog.local_iceberg.warehouse", "./local_iceberg_warehouse")
    .getOrCreate()
)

# LakeSail reads the in-memory Arrow stream from Pathway without copying data
df = spark.readStream.format("arrow").load(arrow_stream)

# Perform pre-processing / transformation using PySpark syntax
clean_df = df.filter(df.event_id.isNotNull()).withColumn(
    "processed_at", spark.functions.current_timestamp()
)


# ==========================================
# 3. STORAGE (Iceberg)
# ==========================================
# Stream the processed data directly into your local Iceberg table
query = (
    clean_df.writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "./local_iceberg_warehouse/_checkpoints")
    .toTable("local_iceberg.db.events_buffer")
)

query.awaitTermination()
