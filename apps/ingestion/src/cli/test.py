import structlog
import typer

from src.core.contexts.builder import JobContextBuilder
from src.features.regression.regression import run_comparison

LOG = structlog.get_logger(__name__)

test_app = typer.Typer(help="Testing and validation utilities.")

def get_ctx(job_id: str, dataset: str):
    """One place to handle all config resolution for test commands."""
    return JobContextBuilder().build(job_id=job_id, dataset_id=dataset)

@test_app.command(name="compare")
def test_regression(
    job_id: str = typer.Option(..., "--job-id", help="The job identifier"),
    dataset: str = typer.Option(..., "--dataset", help="The dataset identifier"),
    master_path: str = typer.Option(
        ..., "--master-path", help="S3/Local path for baseline data"
    ),
    feature_path: str = typer.Option(
        ..., "--feature-path", help="S3/Local path for new code data"
    ),
    ignore_cols: str | None = typer.Option(
        None, "--ignore-cols", help="Comma-separated columns to skip"
    ),
):
    """
    Run side-by-side data validation between two storage locations.
    """
    # 1. LOAD CONFIG ONCE
    # This resolves app.yaml + job.yaml + environments
    ctx = get_ctx(job_id, dataset)
    
    # 2. If master_path is provided via CLI, override the config URL
    if master_path:
        ctx.archive_config.url = master_path

    # 3. CALL FUNCTIONAL OPS
    ignore_list = [c.strip() for c in ignore_cols.split(",") if c]
    success = run_comparison(ctx, feature_path, ignore_list)
    
    if not success:
        raise typer.Exit(1)
@test_app.command("clone")
def clone_sink(
    source_path: str = typer.Option(..., help="Source Table or S3 Path"),
    target_path: str = typer.Option(..., help="Target Skeleton Path"),
):
    """
    Clones the structure/schema of a sink without copying the underlying data.
    Used to prepare a clean environment for regression testing.
    """
    from src.services.factory import ServiceFactory
    
    LOG.info("Starting skeleton clone", source=source_path, target=target_path)
    
    # 1. Resolve the appropriate service (S3, Snowflake, Postgres, etc.)
    service = ServiceFactory.get_service_from_path(source_path)
    
    # 2. Execute the 'Clone' logic you added to your Service class
    # Ensure your implementation uses 'LIMIT 0' or similar for metadata-only
    try:
        service.clone(source=source_path, target=target_path, metadata_only=True)
        print(f"✅ Successfully created skeleton at: {target_path}")
    except Exception as e:
        LOG.error("Clone failed", error=str(e))
        raise typer.Exit(code=1)

@test_app.command("regression")
def regression(
    job_id: str, 
    dataset: str, 
    feature_path: str, 
    master_path: Optional[str] = None, # Optional: Fallback to app.yaml if not provided
    ignore_cols: str = ""
):
    
@test_app.command("impact")
def test_impact(
    job_id: str,
    dataset: str,
    baseline_tag: str = "latest-master"
):
    """
    Automated Impact Analysis:
    - Runs the target dataset (expected change).
    - Auto-discovers and runs all peer datasets (expected NO change).
    """
    # 1. Discovery
    transformer_name, affected_suite = get_affected_suite(job_id, dataset)
    
    print(f"🎯 Target: {job_id}.{dataset}")
    print(f"🧬 Shared Transformer: {transformer_name}")
    print(f"👥 Peer Datasets Found: {len(affected_suite) - 1}")

    results = []
    for item in affected_suite:
        curr_job, curr_ds = item["job_id"], item["dataset_id"]
        is_target = (curr_job == job_id and curr_ds == dataset)
        
        print(f"\n--- Testing {'[TARGET] ' if is_target else '[PEER]   '} {curr_job}.{curr_ds} ---")
        
        # Logic Regression (Extract-Reuse)
        is_match = run_logic_regression(curr_job, curr_ds, baseline_tag)
        
        if is_target:
            # For target, we expect a difference if logic was updated
            status = "⚠️ Changed (Verify Diffs)" if not is_match else "ℹ️ No Change"
        else:
            # For peers, we expect 1:1 identity
            status = "✅ Stable" if is_match else "❌ SPILLOVER DETECTED"
            
        results.append({"name": f"{curr_job}.{curr_ds}", "status": status, "is_target": is_target})

    # 3. Print Final Report
    print_summary_table(results)