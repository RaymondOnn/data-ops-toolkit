class DatabaseFault(Exception):
    """Base exception for database domain faults."""


class ColumnMismatch(DatabaseFault):
    """Raised when incoming dataset schema does not align with the target table schema."""

    def __init__(
        self,
        target: str,
        missing: set[str] | list[str] | None = None,
        extra: set[str] | list[str] | None = None,
    ):
        missing_str = f" missing: {sorted(missing)}" if missing else ""
        extra_str = f" extra: {sorted(extra)}" if extra else ""
        message = (
            f"Schema mismatch for target table '{target}' -{missing_str}{extra_str}"
        )
        super().__init__(message)


class MissingColumns(ColumnMismatch):
    """Raised when required columns in the target table are missing from the incoming dataset."""

    def __init__(
        self,
        target: str,
        missing: set[str] | list[str],
        available: set[str] | list[str],
    ):
        self.missing = sorted(missing)
        message = (
            f"Dataset is missing required column(s) present in target table '{target}': "
            f"{self.missing}. "
            f"Available columns: {available}"
        )
        DatabaseFault.__init__(self, message)


class TableNotFound(DatabaseFault):
    """Raised when a requested table or view does not exist in the database catalog."""

    def __init__(self, target: str):
        super().__init__(f"Target table or view '{target}' does not exist.")


class TypeIncompatibility(DatabaseFault):
    """Raised when an incoming data type cannot be safely cast or mapped to the database native type."""

    def __init__(self, column: str, source_type: str, target_type: str):
        super().__init__(
            f"Cannot map column '{column}' from source type '{source_type}' "
            f"to target database type '{target_type}'."
        )


class RowCountMismatch(DatabaseFault):
    """Raised when expected row counts do not match actual staging or promotion counts."""

    def __init__(self, target: str, expected: int, actual: int):
        super().__init__(
            f"Row count mismatch for target '{target}': expected {expected}, got {actual}."
        )


class DataDriftDetected(DatabaseFault):
    """Raised when checksum or MINUS comparison checks fail between reference and candidate tables."""

    def __init__(self, ref_table: str, candidate_table: str, drift_count: int):
        super().__init__(
            f"Data drift detected between '{ref_table}' and '{candidate_table}'. "
            f"Found {drift_count} non-matching row(s)."
        )
