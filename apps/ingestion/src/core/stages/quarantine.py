import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

LOG = logger


class QuarantineWriter:
    """Handles writing corrupted or failed records to a Dead Letter Queue (DLQ)."""

    def __init__(self, dlq_path: str, schema: pa.Schema):
        self.dlq_path = dlq_path
        self.schema = schema
        self._writer: pq.ParquetWriter | None = None

    def write_bad_batch(self, batch: pa.RecordBatch, reason: str):
        """Appends the failed batch to the quarantine storage with error metadata."""
        try:
            # Add error metadata column to the batch
            error_col = pa.array([reason] * len(batch), type=pa.string())
            meta_batch = batch.append_column("quarantine_reason", error_col)

            if self._writer is None:
                extended_schema = self.schema.append(
                    pa.field("quarantine_reason", pa.string())
                )
                self._writer = pq.ParquetWriter(self.dlq_path, extended_schema)

            self._writer.write_batch(meta_batch)
            LOG.warning(
                f"Quarantined {len(batch)} records to {self.dlq_path}. Reason: {reason}"
            )
        except Exception:
            LOG.exception("Critical error writing to DLQ")

    def close(self):
        if self._writer:
            self._writer.close()
