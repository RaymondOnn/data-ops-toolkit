import pytest

from src.core.strategies.transform.factory import TransformFactory


def test_factory_get_transformer_success():
    """
    GIVEN a registered transformer type (e.g., 'base_transformer')
    WHEN get_transformer is invoked
    THEN it should return an instance of the requested strategy
    """
    # We assume 'base_transformer' is a standard part of the toolkit
    transformer = TransformFactory.get_transformer(
        "base_transformer", dataset_id="ds1", job_id="j1"
    )

    assert transformer is not None
    assert hasattr(transformer, "apply")
    # Verify it was initialized with the correct IDs for logging
    assert transformer.dataset_id == "ds1"


def test_factory_invalid_type():
    """
    GIVEN a transformer type that is not registered
    WHEN get_transformer is called
    THEN it should raise a ValueError to prevent a broken pipeline run
    """
    with pytest.raises(ValueError, match="Unsupported transformer"):
        TransformFactory.get_transformer("ghost_logic", dataset_id="ds", job_id="job")


def test_factory_empty_type():
    """
    GIVEN an empty or None transformer type
    WHEN get_transformer is called
    THEN it should raise a ValueError
    """
    with pytest.raises(ValueError):
        TransformFactory.get_transformer("", dataset_id="ds", job_id="job")


def test_factory_consistency():
    """
    GIVEN multiple requests for the same transformer
    WHEN get_transformer is called
    THEN it should return a fresh instance of the same implementation class
    """
    t1 = TransformFactory.get_transformer(
        "base_transformer", dataset_id="d", job_id="j"
    )
    t2 = TransformFactory.get_transformer(
        "base_transformer", dataset_id="d", job_id="j"
    )

    assert type(t1) is type(t2)
    assert t1 is not t2  # Strategies are usually fresh instances per request
