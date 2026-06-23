[ Streaming Data Source ]
          │
          ▼
┌───────────────────────────────┐
│ 1. LISTENER (Pathway)         │ ◄── Ingests raw events & handles streaming backpressure
└───────────────────────────────┘
          │ (Micro-batches / Data Streams)
          ▼
┌───────────────────────────────┐
│ 2. PRE-PROCESSOR (LakeSail)   │ ◄── Transforms & cleans data using zero-JVM PySpark
└───────────────────────────────┘
          │ (Structured, clean DataFrames)
          ▼
┌───────────────────────────────┐
│ 3. STORAGE (Iceberg Table)    │ ◄── Local folder (.parquet + metadata) ➔ Scales to S3
└───────────────────────────────┘
