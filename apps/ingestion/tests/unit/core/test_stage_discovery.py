from src.core.stages.contracts.stage import ExecutionStageRegistry
from src.core.stages.types import StageConfig  # noqa: F401


def test_stage_registry():
    """Ensure ExecutionStageRegistry registers and resolves stage classes using .get() and .register()."""
    start_cls = ExecutionStageRegistry.get("start")
    assert start_cls is not None
    assert "start" in ExecutionStageRegistry.registered_stages()
    assert "extract" in ExecutionStageRegistry.registered_stages()
    assert "transform" in ExecutionStageRegistry.registered_stages()
    assert "write" in ExecutionStageRegistry.registered_stages()


def test_stage_registry_decorator():
    """Ensure ExecutionStageRegistry.register() acts as decorator."""

    @ExecutionStageRegistry.register("custom_dummy")
    class DummyStage:
        pass

    assert ExecutionStageRegistry.get("custom_dummy") is DummyStage
    assert "custom_dummy" in ExecutionStageRegistry.registered_stages()
