## Additional Feature Skill Suggestions

To extend your filesystem library capabilities beyond standard file CRUD, consider these complementary skill mixins:

1. FileQuarantineManager ("quarantine")
Purpose: Handles job quarantine operations for invalid/corrupted files during ingestion runs.

Capabilities: Moves dead-letter files to quarantine directories, attaches failure metadata sidecar files, and manages quarantine retention policies.

1. FileChecksumValidator ("checksum")
Purpose: Verifies integrity and idempotency during transfers.

Capabilities: Calculates streaming hashes (MD5, SHA-256), validates remote vs. local checksums, and flags partial or corrupt writes.

1. FileBatchPartitioner ("batch_partitioner")
Purpose: Higher-level extraction of file splitting logic (currently sitting in FileSystemClient.split_by_size).

Capabilities: Groups directories/archives into memory-bounded file batches based on size or record counts before passing them to downstream tasks.

1. FileSchemaInspector ("schema_inspector")
Purpose: Inspects file structure and infers metadata without loading whole datasets into memory.

Capabilities: Reads Parquet metadata footers, inspects CSV headers/delimiter detection, and compares schema drift across multiple files.
