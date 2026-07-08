"""Base check class for all diagnostic checks."""

from abc import ABC, abstractmethod

from libs.network.core.result import DiagnosticResult


class BaseCheck(ABC):
    """Base class for all diagnostic checks."""

    @abstractmethod
    def get_section_name(self) -> str:
        """Get the name of this check section."""
        pass

    @abstractmethod
    def run(self) -> DiagnosticResult:
        """Execute the diagnostic check."""
        pass

    def _success(
        self,
        report: str,
        recommendation: str | None = None,
        bash_command: str | None = None,
    ) -> DiagnosticResult:
        """Create a successful result."""
        return DiagnosticResult(
            section=self.get_section_name(),
            passed=True,
            report=report,
            recommendation=recommendation,
            bash_command=bash_command,
        )

    def _failure(
        self,
        report: str,
        recommendation: str | None = None,
        bash_command: str | None = None,
        auto_fix: bool = False,
        fix_command: str | None = None,
        impact_level: str = "MEDIUM",
    ) -> DiagnosticResult:
        """Create a failed result."""
        return DiagnosticResult(
            section=self.get_section_name(),
            passed=False,
            report=report,
            recommendation=recommendation,
            bash_command=bash_command,
            auto_fix=auto_fix,
            fix_command=fix_command,
            impact_level=impact_level,
        )
