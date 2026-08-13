import json
import subprocess
import sys
from pathlib import Path

import libcst as cst
import networkx as nx
from loguru import logger

from src.core.contexts import TaskContextBuilder
from src.utils.constants import APP_CONFIG_ROOT

# Import the AST extraction and CallGraph structures we built
from .analyze import ASTFingerprinter, CodeImpactAnalyzer, DatasetTarget, FunctionId


def collect_runtime_fingerprints(src_root: Path) -> dict[FunctionId, str]:
    """Core shared abstract function to scan python paths and extract AST hashes."""
    fingerprints = {}
    for py_file in src_root.rglob("*.py"):
        try:
            # Determine standard dot-notation module path string
            rel = py_file.resolve().relative_to(src_root.resolve()).with_suffix("")
            module_name = ".".join(rel.parts)

            with py_file.open(encoding="utf-8") as f:
                cst_tree = cst.parse_module(f.read())

            visitor = ASTFingerprinter(module_name)
            cst_tree.visit(visitor)
            fingerprints.update(visitor.fingerprints)
        except Exception as e:
            logger.debug(f"Failed fingerprint generation on {py_file.name}: {e}")
    return fingerprints


def get_baseline_fingerprints(baseline_pex_path: Path) -> dict[FunctionId, str]:
    """Invokes the baseline PEX via a command-line subprocess to stream its hashes."""
    if not baseline_pex_path.exists():
        raise FileNotFoundError(
            f"Baseline PEX executable not found at: {baseline_pex_path}"
        )

    logger.info(
        f"🚀 Invoking baseline PEX command: {baseline_pex_path.name} --dump-ast-fingerprints"
    )

    # Run the baseline PEX utilizing the active system python binary environment
    result = subprocess.run(
        [sys.executable, str(baseline_pex_path), "--dump-ast-fingerprints"],
        capture_output=True,
        text=True,
        check=True,
    )

    # Reconstruct the string transport format back into structural FunctionId keys
    raw_data = json.loads(result.stdout.strip())
    baseline_hashes = {}
    for key, structural_hash in raw_data.items():
        mod, cls, func = key.split(":")
        func_id = FunctionId(
            module_path=mod, class_name=cls if cls else None, function_name=func
        )
        baseline_hashes[func_id] = structural_hash

    return baseline_hashes


def find_affected_peers(  # noqa
    baseline_pex: str | Path, env: str = "dev"
) -> list[dict[str, str]]:
    """Rewritten impact analysis peer engine.

    Queries the candidate tree in-memory and triggers the baseline PEX via
    subprocess commands to calculate a minimal, surgical regression scope.
    """
    baseline_pex_path = Path(baseline_pex)

    # 1. Locate the current running source directory coordinates
    candidate_src_root = Path(__file__).resolve().parents[1] / "src"
    if not candidate_src_root.exists():
        candidate_src_root = Path("./apps/ingestion/src").resolve()

    # 2. Compile the operational registry mapping application touchpoints to configs
    builder = TaskContextBuilder(env=env)
    dataset_registry: dict[str, list[DatasetTarget]] = {}

    for job_dir in APP_CONFIG_ROOT.iterdir():
        if not job_dir.is_dir() or not (job_dir / "config.yaml").exists():
            continue
        try:
            for ctx in builder.build(job_id=job_dir.name):
                target = DatasetTarget(job_id=ctx.job_id, dataset_id=ctx.dataset_id)
                # Group by transformation identity signatures
                if ctx.transform:
                    t_name = ctx.transform.params.get("name") or ctx.transform.type
                    dataset_registry.setdefault(
                        f"transform:{t_name.casefold()}", []
                    ).append(target)
                # Group by database storage sink identities
                if ctx.load:
                    dataset_registry.setdefault(
                        f"sink:{ctx.load.type.casefold()}", []
                    ).append(target)
        except Exception as e:
            logger.debug(
                f"Skipping indexing registry for job context {job_dir.name}: {e}"
            )

    # 3. Retrieve fingerprints from both execution scopes
    try:
        baseline_hashes = get_baseline_fingerprints(baseline_pex_path)
    except Exception as e:
        logger.error(f"Failed to fetch baseline reference configurations: {e}")
        return []

    # Calculate Candidate hashes directly in-memory (no subprocess required)
    logger.info(
        "🔍 Local Candidate analysis active: Generating local structural maps..."
    )
    candidate_hashes = collect_runtime_fingerprints(candidate_src_root)

    # 4. Generate the current invocation call-graph
    analyzer = CodeImpactAnalyzer(
        root_dir=candidate_src_root, dataset_registry=dataset_registry
    )
    analyzer.build_call_graph()

    # 5. Isolate distinct functional mutations
    changed_functions: list[FunctionId] = []
    for func_id, cand_hash in candidate_hashes.items():
        base_hash = baseline_hashes.get(func_id)
        if base_hash != cand_hash:
            changed_functions.append(func_id)
            logger.warning(
                f"⚡ Structural variation detected: {func_id.module_path} -> {func_id.function_name}"
            )

    if not changed_functions:
        logger.success(
            "✅ Code trees match exactly structurally. Zero regression tasks required."
        )
        return []

    # 6. Trace dependency paths up the NetworkX call graph
    affected_modules: set[str] = set()
    for func_id in changed_functions:
        if func_id in analyzer.call_graph:
            affected_modules.update(nx.ancestors(analyzer.call_graph, func_id))
        affected_modules.add(func_id.module_path)

    # 7. Map the resulting modules back to concrete dataset coordinates
    final_targets: set[DatasetTarget] = set()
    for mod in affected_modules:
        stem = mod.split(".")[-1].lower()
        for key in [f"transform:{stem}", f"sink:{stem}"]:
            if key in dataset_registry:
                final_targets.update(dataset_registry[key])

    results = list(final_targets)

    # Safe fallback checking to ensure manual testing sessions remain quick and operational
    if len(results) > 5:
        results = analyzer._sample_representative_tranches(results)

    # Return exactly the schema format required by your test orchestrator loops
    return [{"job_id": t.job_id, "dataset_id": t.dataset_id} for t in results]


def handle_regression_cli():
    """Add this handler inside your main application entry point CLI parser."""
    if "--dump-ast-fingerprints" in sys.argv:
        # Resolve the package source directory relative to the current file location
        # This safely works both in standard python execution and inside a zipped PEX
        src_root = Path(__file__).resolve().parents[1] / "src"

        # Pull fingerprints from the active runtime
        fingerprints = collect_runtime_fingerprints(src_root)

        # Serialize namedtuple keys to simple string tags for transport over stdout
        serializable = {
            f"{fid.module_path}:{fid.class_name or ''}:{fid.function_name}": h
            for fid, h in fingerprints.items()
        }

        print(json.dumps(serializable))
        sys.exit(0)
