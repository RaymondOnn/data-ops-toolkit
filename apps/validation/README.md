Stateless Data Auditor (SDA)
A High-Performance, Multi-Tier Data Validation Engine

🎯 The Challenge
Validating data migrations or ingestion jobs often requires expensive, multiple passes over production databases, leading to compute bloat and performance degradation. The challenge was to build a platform-agnostic tool capable of validating 10,000,000+ rows within a strict 512MB RAM budget, while adhering to a Max-1-Query constraint per table.

🏗️ Architecture: The 4-Gate Defense
The SDA uses a hierarchical validation strategy. Each tier acts as a gate, escalating in complexity only when necessary.

Tier 0: Schema Contract – Validates metadata (column names, types, nullability) via system catalogs.

Tier 1: Volume & Totals – Heuristic checks (COUNT, SUM, SUM(LENGTH)) captured during a single streaming pass.

Tier 2: Bit-Level Integrity – A deterministic BITXOR_AGG hash signature ensuring every bit of data moved correctly.

Tier 3: Health Profiling – Statistical distribution analysis (MIN, MAX, NULL_COUNT) to provide root-cause context for failures.

🛠️ Tech Stack & Rationale
Polars (Lazy API): Leveraged for its streaming engine to process multi-gigabyte datasets in small memory chunks.

ADBC (Arrow Database Connectivity): Ensures zero-copy data transfer, bypassing the Python object overhead that usually triggers OOMs.

Diskcache: Serves as a "Thin Index" for the Balance Sheet algorithm, allowing row-level diffing without in-memory joins.

Local Parquet Caching: Uses a "Tee-split" strategy to persist source data to disk during the first scan, satisfying the one-query constraint.