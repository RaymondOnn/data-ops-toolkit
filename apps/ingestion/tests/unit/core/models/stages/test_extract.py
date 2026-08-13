from unittest.mock import MagicMock

import polars as pl
import pytest

from src.core.models.stages.extract import ExtractStage
from src.core.stages.enums import Stage


@pytest.fixture
def extract_stage():
    return ExtractStage(Stage.EXTRACT)


def test_merge_schemas_basic(extract_stage):
    """
    GIVEN a list of schemas from multiple parquet files
    WHEN _merge_schemas is called
    THEN it should return a union of all columns
    """
    schemas = [{"id": pl.Int64, "name": pl.String}, {"id": pl.Int64, "age": pl.Int32}]
    merged = extract_stage._merge_schemas(schemas)
    assert "id" in merged
    assert "name" in merged
    assert "age" in merged
    assert merged["name"] == "String"


def test_resolve_audit_identity_single_file(extract_stage):
    """
    GIVEN a single source file in the context
    WHEN _resolve_audit_identity is called
    THEN it should return the filename as the source identifier
    """
    ctx = MagicMock()
    ctx.resource = "s3://bucket/raw/data.csv"
    source_files = ["s3://bucket/raw/data.csv"]

    ident = extract_stage._resolve_audit_identity(ctx, source_files)
    assert ident == "data.csv"


def test_resolve_audit_identity_batch(extract_stage):
    """
    GIVEN multiple source files (a batch)
    WHEN _resolve_audit_identity is called
    THEN it should return the parent folder name as the source identifier
    """
    ctx = MagicMock()
    ctx.resource = "s3://bucket/raw_folder/"
    source_files = ["file1.csv", "file2.csv"]

    ident = extract_stage._resolve_audit_identity(ctx, source_files)
    assert ident == "raw_folder"


def test_calculate_checksum(extract_stage, tmp_path):
    """
    GIVEN a physical file
    WHEN _calculate_checksum is called
    THEN it should return the correct MD5 hash
    """
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello world")

    checksum = extract_stage._calculate_checksum(test_file)
    # md5 of 'hello world'
    assert checksum == "5eb63bbbe01eeed093cb22bb8f5acdc3"
