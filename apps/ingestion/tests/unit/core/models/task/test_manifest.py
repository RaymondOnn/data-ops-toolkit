import msgspec
from apps.ingestion.src.core.models.task.manifest import ExtractPayload, TaskManifest
from apps.ingestion.src.core.models.task.status import ExecutionStatus


def test_manifest_is_fully_populated_false():
    """
    GIVEN a TaskManifest with only the header and one stage payload
    WHEN is_fully_populated is checked
    THEN it should return False
    """
    manifest = TaskManifest(
        job_id="j1",
        run_id="r1",
        dataset_id="d1",
        status=ExecutionStatus.PENDING,
        current_stage="extract",
        bitmask=1,
        extract=ExtractPayload(source_count=100),
    )

    assert manifest.is_fully_populated() is False


def test_manifest_serialization():
    """
    GIVEN a TaskManifest instance
    WHEN encoded to JSON
    THEN it should maintain field naming and omit optional nulls
    """
    manifest = TaskManifest(
        job_id="j1",
        run_id="r1",
        dataset_id="d1",
        status=ExecutionStatus.SUCCESS,
        current_stage="archive",
        bitmask=255,
    )

    encoded = msgspec.json.encode(manifest)
    decoded = msgspec.json.decode(encoded, type=dict)

    assert decoded["job_id"] == "j1"
    assert decoded["status"] == "success"
    # Ensure optional fields that are None are present as null or absent
    # depending on msgspec config (defaults to present as null for msgspec.Struct)
    assert "extract" in decoded
    assert decoded["extract"] is None


def test_extract_payload_defaults():
    """
    GIVEN an ExtractPayload with minimal info
    WHEN accessed
    THEN it should have valid default values for list fields
    """
    payload = ExtractPayload(artifact_folder="/tmp")
    assert payload.files == []
    assert payload.file_count == 0
