"""
Network diagnostics library for corporate environments.

This package provides comprehensive network diagnostics for data ingestion pipelines,
specifically designed for corporate environments with CNTLM proxies, VPNs, firewalls,
and complex authentication requirements.
"""

from pathlib import Path

from .core import DiagnosticResult, NetworkDoctor
from .exceptions import (
    CheckDependencyError,
    CheckExecutionError,
    NetworkDiagnosticError,
    ReporterError,
)

__version__ = "1.0.0"

__all__ = [
    "CheckDependencyError",
    "CheckExecutionError",
    "DiagnosticResult",
    "NetworkDiagnosticError",
    "NetworkDoctor",
    "ReporterError",
]


# Convenience functions
# Convenience functions
def quick_check(
    target_host: str,
    target_port: int,
    proxy_url: str = "http://127.0.0.1:3128",
    auto_fix: bool = False,
) -> bool:
    """Quick diagnostic check for common network issues."""
    doctor = NetworkDoctor(target_host, target_port, proxy_url, auto_fix=auto_fix)
    return doctor.run_diagnostics()


def diagnose_and_export(
    target_host: str,
    target_port: int,
    output_path: str | Path,
    proxy_url: str = "http://127.0.0.1:3128",
    auto_fix: bool = False,
    format_: str = "json",  # json, html
) -> Path:
    """Run diagnostics and export report."""
    doctor = NetworkDoctor(target_host, target_port, proxy_url, auto_fix=auto_fix)
    doctor.run_diagnostics()

    if format_.lower() == "html":
        return doctor.export_html(output_path)
    return doctor.export_json(output_path)
