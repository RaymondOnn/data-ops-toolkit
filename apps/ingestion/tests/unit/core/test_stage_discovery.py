from src.core.stages.contracts.stage import ExecutionStage
from src.core.stages.types import StageConfig  # noqa: F401


def test_stage_registry():
    """Ensure ExecutionStageRegistry registers and resolves stage classes using .get() and .register()."""
    start_cls = ExecutionStage.get("start")
    assert start_cls is not None
    assert "start" in ExecutionStage.registered_stages()
    assert "extract" in ExecutionStage.registered_stages()
    assert "transform" in ExecutionStage.registered_stages()
    assert "write" in ExecutionStage.registered_stages()


def test_stage_registry_decorator():
    """Ensure ExecutionStageRegistry.register() acts as decorator."""

    @ExecutionStage.register("custom_dummy")
    class DummyStage:
        pass

    assert ExecutionStage.get("custom_dummy") is DummyStage
    assert "custom_dummy" in ExecutionStage.registered_stages()
