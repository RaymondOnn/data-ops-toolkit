"""NetworkDoctor main class orchestrating all diagnostics."""

import logging
import platform
from pathlib import Path
from typing import TYPE_CHECKING, Any

from libs.network.checks import create_all_checks
from libs.network.reporters import ConsoleReporter, HtmlReporter, JsonReporter

from .pipeline import DiagnosticPipeline

if TYPE_CHECKING:
    from .result import DiagnosticResult

LOG = logging.getLogger(__name__)


class NetworkDoctor:
    """
    Comprehensive network diagnostic suite for corporate environments.

    Designed for environments with:
    - CNTLM/NTLM proxies
    - Corporate VPNs
    - Firewalls and security groups
    - SSL inspection
    - Complex DNS configurations
    """

    def __init__(
        self,
        target_host: str,
        target_port: int,
        proxy_url: str = "http://127.0.0.1:3128",
        vpn_prefixes: list[str] | None = None,
        debug_traffic: bool = False,
        auto_fix: bool = False,
    ):
        """
        Initialize the NetworkDoctor.

        Args:
            target_host: Target service hostname or IP.
            target_port: Target service port.
            proxy_url: Local proxy URL.
            vpn_prefixes: Corporate VPN IP prefixes.
            debug_traffic: Enable traffic inspection.
            auto_fix: Attempt automatic fixes.
        """
        self.target_host = target_host
        self.target_port = target_port
        self.proxy_url = proxy_url
        self.debug_traffic = debug_traffic
        self.auto_fix = auto_fix
        self.vpn_prefixes = vpn_prefixes or ["10.", "172.16.", "192.168."]

        self._pipeline = DiagnosticPipeline()
        self._results: list[DiagnosticResult] = []

        self._initialize_checks()

    def _initialize_checks(self) -> None:
        """Register all diagnostic checks."""
        checks = create_all_checks(
            target_host=self.target_host,
            target_port=self.target_port,
            proxy_url=self.proxy_url,
            vpn_prefixes=self.vpn_prefixes,
            debug_traffic=self.debug_traffic,
        )
        self._pipeline.register_all(checks)

    def run_diagnostics(self) -> bool:
        """Run all diagnostic checks."""
        self._log_header()

        self._results = self._pipeline.run(auto_fix=self.auto_fix)

        all_passed = self._pipeline.get_passed()
        self._log_footer(all_passed)

        return all_passed

    def get_report(self) -> dict[str, Any]:
        """
        Generate a complete diagnostic report.

        Returns:
            dict[str, Any]: Full report with metadata and results.
        """
        return {
            "metadata": {
                "target_host": self.target_host,
                "target_port": self.target_port,
                "proxy_url": self.proxy_url,
                "auto_fix": self.auto_fix,
                "timestamp": self._get_timestamp(),
                "platform": platform.system(),
                "os_release": platform.release(),
                "python_version": platform.python_version(),
            },
            "results": [r.to_dict() for r in self._results],
            "all_passed": self._pipeline.get_passed(),
        }

    def export_json(self, output_path: Path | str) -> Path:
        """Export diagnostic report to JSON."""
        reporter = JsonReporter(output_path)
        return reporter.export(self.get_report())

    def export_html(
        self, output_path: Path | str, title: str = "Network Diagnostic Report"
    ) -> Path:
        """Export diagnostic report to HTML."""

        reporter = HtmlReporter(output_path, title=title)
        return reporter.export(self.get_report())

    def print_report(self) -> None:
        """Print report to console."""
        reporter = ConsoleReporter()
        reporter.export(self.get_report())

    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import UTC, datetime

        return datetime.now(UTC).isoformat()

    def _log_header(self) -> None:
        """Log diagnostic header."""
        LOG.info("=" * 70)
        LOG.info("🚀 CORPORATE NETWORK DIAGNOSTIC SUITE")
        LOG.info("Target Destination : %s:%d", self.target_host, self.target_port)
        LOG.info("Proxy              : %s", self.proxy_url)
        LOG.info("Auto-Fix Mode      : %s", "ON" if self.auto_fix else "OFF")
        LOG.info("Platform           : %s %s", platform.system(), platform.release())
        LOG.info("=" * 70)

    def _log_footer(self, all_passed: bool) -> None:
        """Log diagnostic footer."""
        LOG.info("=" * 70)
        if all_passed:
            LOG.info("🎉 All checks passed - Environment is healthy")
        else:
            LOG.critical("🚨 Some checks failed")
            if not self.auto_fix:
                LOG.info("💡 Run with --auto-fix to attempt automatic fixes")
        LOG.info("=" * 70)
