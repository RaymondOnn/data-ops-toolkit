"""Diagnostic pipeline management."""

from collections.abc import Callable
from dataclasses import dataclass, field

from .result import DiagnosticResult


@dataclass
class CheckDefinition:
    """Definition of a diagnostic check."""

    section: str
    method: Callable[[], DiagnosticResult]
    dependencies: list[str] = field(default_factory=list)
    required: bool = True


class DiagnosticPipeline:
    """Manages the execution of diagnostic checks with dependencies."""

    def __init__(self):
        self._checks: dict[str, CheckDefinition] = {}
        self._results: dict[str, DiagnosticResult] = {}
        self._skipped: list[str] = []

    def register(self, check: CheckDefinition) -> None:
        """Register a diagnostic check."""
        self._checks[check.section] = check

    def register_all(self, checks: list[CheckDefinition]) -> None:
        """Register multiple diagnostic checks."""
        for check in checks:
            self.register(check)

    def run(self, auto_fix: bool = False) -> list[DiagnosticResult]:
        """
        Run all registered checks respecting dependencies.

        Args:
            auto_fix: Whether to attempt automatic fixes.

        Returns:
            list[DiagnosticResult]: All results (including skipped checks).
        """
        self._results = {}
        self._skipped = []

        for section, check in self._checks.items():
            if self._should_skip(check):
                self._skipped.append(section)
                continue

            try:
                result = check.method()
                self._results[section] = result

                # Auto-fix if enabled
                if (
                    auto_fix and not result.passed and result.fix_command
                ) and self._attempt_fix(result, section):
                    # Re-run check
                    result = check.method()
                    self._results[section] = result

            except Exception as e:
                self._results[section] = DiagnosticResult.from_failure(
                    section=section,
                    report=f"Unhandled exception: {e!s}",
                    recommendation="Run with --debug flag for detailed traceback",
                )

        return self.get_all_results()

    def _should_skip(self, check: CheckDefinition) -> bool:
        """Determine if a check should be skipped due to dependency failure."""
        if not check.dependencies:
            return False

        for dep in check.dependencies:
            if dep not in self._results:
                # Dependency hasn't run yet
                return True
            if not self._results[dep].passed:
                # Dependency failed
                return True
        return False

    def _attempt_fix(self, result: DiagnosticResult, section: str) -> bool:
        """Attempt to fix a failed check."""
        if not result.fix_command:
            return False

        import logging
        import subprocess

        logger = logging.getLogger(__name__)
        logger.info("🔧 Attempting auto-fix for %s...", section)

        try:
            subprocess.run(
                result.fix_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            return True
        except Exception as e:
            logger.warning("Auto-fix failed: %s", e)
            return False

    def get_all_results(self) -> list[DiagnosticResult]:
        """Get all results including skipped checks."""
        results = []

        for section, _ in self._checks.items():
            if section in self._skipped:
                results.append(
                    DiagnosticResult.from_failure(
                        section=section,
                        report="Skipped due to dependency failure",
                        impact_level="LOW",
                    )
                )
            elif section in self._results:
                results.append(self._results[section])

        return results

    def get_passed(self) -> bool:
        """Check if all required checks passed."""
        results = self.get_all_results()
        if not results:
            return False

        # Check only required checks that weren't skipped
        required_results = [
            r
            for r in results
            if r.section not in self._skipped
            and self._checks.get(r.section, CheckDefinition("", lambda: None)).required
        ]

        return all(r.passed for r in required_results)
