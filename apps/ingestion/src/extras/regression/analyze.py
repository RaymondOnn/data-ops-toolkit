import ast
import hashlib
from pathlib import Path
from typing import NamedTuple

import libcst as cst
import networkx as nx
from loguru import logger


# Type definitions for internal registry tracking
class DatasetTarget(NamedTuple):
    job_id: str
    dataset_id: str


class FunctionId(NamedTuple):
    module_path: str
    class_name: str | None
    function_name: str


class ASTFingerprinter(cst.CSTVisitor):
    """Extracts structural hashes of functions/methods, ignoring docstrings and comments."""

    def __init__(self, module_path: str):
        self.module_path = module_path
        self.current_class: str | None = None
        self.fingerprints: dict[FunctionId, str] = {}

    def visit_class_def(self, node: cst.ClassDef) -> bool | None:
        self.current_class = node.name.value
        return True

    def leave_class_def(self, original_node: cst.ClassDef) -> None:
        self.current_class = None

    def visit_function_def(self, node: cst.FunctionDef) -> bool | None:
        # Clone the node body to strip docstrings cleanly without breaking mutation contracts
        body_elements = list(node.body.body)
        if body_elements:
            first_stmt = body_elements[0]
            if (
                isinstance(first_stmt, cst.SimpleStatementLine)
                and isinstance(first_stmt.body[0], cst.Expr)
                and isinstance(first_stmt.body[0].value, cst.SimpleString)
            ):
                body_elements.pop(0)

        # Reconstruct a structural node purely to fingerprint logical changes
        structural_node = node.with_changes(
            body=node.body.with_changes(body=body_elements),
            leading_lines=(),
        )

        # Hash the concrete syntax string structure
        node_code = cst.Module((structural_node,)).code
        structural_hash = hashlib.sha256(node_code.encode("utf-8")).hexdigest()

        func_id = FunctionId(
            module_path=self.module_path,
            class_name=self.current_class,
            function_name=node.name.value,
        )
        self.fingerprints[func_id] = structural_hash
        return False  # Inner functions don't need standalone tracking


class CallGraphBuilder(ast.NodeVisitor):
    """Builds a map of module/method invocations using standard AST tracking."""

    def __init__(self, current_file: Path, root_path: Path):
        self.current_file = current_file.resolve()
        self.root_path = root_path.resolve()
        self.invocations: set[tuple[str, str | None, str]] = set()
        self.imported_names: dict[
            str, tuple[str, str | None]
        ] = {}  # local_name -> (module, class)

    def visit_import_from(self, node: ast.ImportFrom):
        if not node.module:
            return
        for alias in node.names:
            local_name = alias.asname or alias.name
            # Resolve possible structural components (e.g., from utils.math import transform)
            self.imported_names[local_name] = (node.module, alias.name)

    def visit_import(self, node: ast.Import):
        for alias in node.names:
            local_name = alias.asname or alias.name
            self.imported_names[local_name] = (alias.name, None)

    def visit_call(self, node: ast.Call):
        # Case 1: Direct function call, e.g., run_transformation()
        if isinstance(node.func, ast.Name):
            name = node.func.id
            if name in self.imported_names:
                mod, item = self.imported_names[name]
                self.invocations.add((mod, None, item or name))

        # Case 2: Method call, e.g., transformer.execute() or Class.method()
        elif isinstance(node.func, ast.Attribute) and isinstance(
            node.func.value, ast.Name
        ):
            obj_name = node.func.value.id
            method_name = node.func.attr
            if obj_name in self.imported_names:
                mod, cls_name = self.imported_names[obj_name]
                self.invocations.add((mod, cls_name, method_name))

        self.generic_visit(node)


class CodeImpactAnalyzer:
    """Computes code blast-radius from structural changes using AST hashing and Call Graphs."""

    def __init__(
        self, root_dir: Path, dataset_registry: dict[str, list[DatasetTarget]]
    ):
        self.root_dir = root_dir.resolve()
        self.dataset_registry = dataset_registry  # Maps component string (e.g. 'transform:my_transform') to targets
        self.call_graph = nx.DiGraph()

    def _get_module_name(self, file_path: Path) -> str:
        """Converts local file path back to standardized python import module format."""
        rel = file_path.resolve().relative_to(self.root_dir).with_suffix("")
        return ".".join(rel.parts)

    def build_call_graph(self):
        """Scans python targets to track function connections across the ecosystem."""
        for py_file in self.root_dir.rglob("*.py"):
            try:
                with py_file.open(encoding="utf-8") as f:
                    tree = ast.parse(f.read(), filename=str(py_file))

                visitor = CallGraphBuilder(py_file, self.root_dir)
                visitor.visit(tree)

                current_mod = self._get_module_name(py_file)
                for target_mod, target_cls, target_func in visitor.invocations:
                    # Edge goes from the code element changed up to the script invoking it
                    self.call_graph.add_edge(
                        FunctionId(target_mod, target_cls, target_func), current_mod
                    )
            except Exception as e:
                logger.debug(f"Failed parsing call graph for {py_file}: {e}")

    def compute_fingerprints(self, directory: Path) -> dict[FunctionId, str]:
        """Generates structural hashes for all methods found under the target directory."""
        fingerprints = {}
        for py_file in directory.rglob("*.py"):
            try:
                module_name = self._get_module_name(py_file)
                with py_file.open(encoding="utf-8") as f:
                    cst_tree = cst.parse_module(f.read())

                visitor = ASTFingerprinter(module_name)
                cst_tree.visit(visitor)
                fingerprints.update(visitor.fingerprints)
            except Exception as e:
                logger.debug(f"Fingerprint failed for {py_file}: {e}")
        return fingerprints

    def analyze(
        self, baseline_dir: Path, max_safe_threshold: int = 5
    ) -> list[DatasetTarget]:
        """Compares current tree vs baseline to safely narrow down candidate datasets."""
        logger.info("🛠️ Building operational call graphs...")
        self.build_call_graph()

        logger.info("🔑 Computing AST fingerprints for Baseline and Candidates...")
        baseline_hashes = self.compute_fingerprints(baseline_dir)
        candidate_hashes = self.compute_fingerprints(self.root_dir)

        # Identify exact changed functions
        changed_functions: list[FunctionId] = []
        for func_id, cand_hash in candidate_hashes.items():
            base_hash = baseline_hashes.get(func_id)
            if base_hash != cand_hash:
                changed_functions.append(func_id)
                logger.warning(
                    f"⚡ Structural change detected: {func_id.module_path}.{f'{func_id.class_name}.' if func_id.class_name else ''}{func_id.function_name}"
                )

        if not changed_functions:
            logger.success(
                "✅ No functional or structural changes detected between environments."
            )
            return []

        # Find affected high level pipeline components
        affected_modules: set[str] = set()
        for func_id in changed_functions:
            if func_id in self.call_graph:
                # Traverse upward using NetworkX to track down every module executing the path
                ancestors = nx.ancestors(self.call_graph, func_id)
                affected_modules.update(ancestors)
            # Add the file itself if it represents a high-level module boundary
            affected_modules.add(func_id.module_path)

        # Map impacted application modules back to target data configurations
        final_targets: set[DatasetTarget] = set()
        for mod in affected_modules:
            stem = mod.split(".")[-1].lower()
            # Match against your pipeline configuration contexts (transform types or sink definitions)
            for key in [f"transform:{stem}", f"sink:{stem}"]:
                if key in self.dataset_registry:
                    final_targets.update(self.dataset_registry[key])

        results = list(final_targets)

        # Enforce sampling trigger if blast radius is impractical for manual tracking
        if len(results) > max_safe_threshold:
            logger.warning(
                f"⚠️ Practical execution limit exceeded! Blast radius spans {len(results)} datasets."
            )
            return self._sample_representative_tranches(results)

        return results

    def _sample_representative_tranches(
        self, targets: list[DatasetTarget]
    ) -> list[DatasetTarget]:
        """Groups data pipelines by Job ID and samples one representative target from each group."""
        grouped: dict[str, list[DatasetTarget]] = {}
        for target in targets:
            grouped.setdefault(target.job_id, []).append(target)

        sampled: list[DatasetTarget] = []
        for job_id, entries in grouped.items():
            sampled.append(entries[0])  # Select first representative pipeline target
            logger.info(
                f"⚖️ Sampled representative candidate from Job [{job_id}]: {entries[0].dataset_id}"
            )

        return sampled
