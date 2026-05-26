from pathlib import Path

import msgspec
from apps.ingestion.src.core.strategies.transform.base import TransformContext


def test_transform_context_initialization():
    """
    GIVEN transformation parameters and directory paths
    WHEN TransformContext is instantiated
    THEN it should correctly store the configuration for Ray workers
    """
    ctx = TransformContext(
        options={"mode": "strict", "threshold": 0.5},
        source_dir=Path("/tmp/extract/run123"),
        destination_dir=Path("/tmp/transform/run123"),
        output_format="parquet",
        type="base_transformer",
    )

    assert ctx.options["mode"] == "strict"
    assert ctx.output_format == "parquet"
    assert ctx.type == "base_transformer"
    assert isinstance(ctx.source_dir, Path)


def test_transform_context_serialization():
    """
    GIVEN a TransformContext instance
    WHEN encoded via msgspec for Ray worker distribution
    THEN it should be perfectly restorable on the remote node
    """
    ctx = TransformContext(
        options={"param": "val"},
        source_dir=Path("/vault/s"),
        destination_dir=Path("/vault/d"),
        output_format="parquet",
        type="ray_test",
    )

    # This mimics the Ray task submission handshake
    encoded = msgspec.json.encode(ctx)
    decoded = msgspec.json.decode(encoded, type=TransformContext)

    assert decoded.type == "ray_test"
    assert decoded.options["param"] == "val"
    # Verify Path objects are restored correctly from strings
    assert isinstance(decoded.source_dir, Path)
    assert decoded.source_dir == Path("/vault/s")
