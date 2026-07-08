"""Diagnostic result dataclasses and utilities."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class DiagnosticResult:
    """Result of a single diagnostic check."""

    section: str
    passed: bool
    report: str
    recommendation: str | None = None
    bash_command: str | None = None
    auto_fix: bool = False
    fix_command: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    impact_level: str = "MEDIUM"  # HIGH, MEDIUM, LOW
    dependencies: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to serializable dictionary."""
        result = {
            "section": self.section,
            "passed": self.passed,
            "report": self.report,
            "timestamp": self.timestamp.isoformat(),
            "impact_level": self.impact_level,
        }
        if self.recommendation:
            result["recommendation"] = self.recommendation
        if self.bash_command:
            result["bash_command"] = self.bash_command
        if self.auto_fix and self.fix_command:
            result["fix_command"] = self.fix_command
        return result

    @classmethod
    def from_success(cls, section: str, report: str) -> "DiagnosticResult":
        """Create a successful result."""
        return cls(section=section, passed=True, report=report)

    @classmethod
    def from_failure(
        cls,
        section: str,
        report: str,
        recommendation: str | None = None,
        bash_command: str | None = None,
        auto_fix: bool = False,
        fix_command: str | None = None,
        impact_level: str = "MEDIUM",
    ) -> "DiagnosticResult":
        """Create a failed result."""
        return cls(
            section=section,
            passed=False,
            report=report,
            recommendation=recommendation,
            bash_command=bash_command,
            auto_fix=auto_fix,
            fix_command=fix_command,
            impact_level=impact_level,
        )
