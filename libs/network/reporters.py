"""Base reporter interface."""

import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any


class BaseReporter(ABC):
    """Base class for diagnostic reporters."""

    @abstractmethod
    def export(self, report: dict[str, Any]) -> Any:
        """Export the report in the reporter's format."""
        pass


class ConsoleReporter(BaseReporter):
    """Print diagnostic results to console."""

    def export(self, report: dict[str, Any]) -> None:
        """Print report to console."""
        print("\n" + "=" * 70)
        print("📊 DIAGNOSTIC REPORT")
        print("=" * 70)

        metadata = report.get("metadata", {})
        print(f"Target: {metadata.get('target_host')}:{metadata.get('target_port')}")
        print(f"Proxy: {metadata.get('proxy_url')}")
        print(f"Platform: {metadata.get('platform')} {metadata.get('os_release')}")
        print("-" * 70)

        for result in report.get("results", []):
            status = "✅ PASS" if result["passed"] else "❌ FAIL"
            print(f"\n{status} | {result['section']}")
            print(f"   {result['report']}")

            if not result["passed"] and "recommendation" in result:
                print(f"   💡 {result['recommendation']}")

            if not result["passed"] and "bash_command" in result:
                print(f"   🐚 {result['bash_command']}")

        print("\n" + "=" * 70)
        all_passed = report.get("all_passed", False)
        if all_passed:
            print("🎉 All checks passed!")
        else:
            print("🚨 Some checks failed - See details above")
        print("=" * 70 + "\n")


class JsonReporter(BaseReporter):
    """Export diagnostic results to JSON file."""

    def __init__(self, output_path: Path | str):
        self.output_path = Path(output_path)

    def export(self, report: dict[str, Any]) -> Path:
        """Export report to JSON file."""
        self.output_path.write_text(
            json.dumps(report, indent=4),
            encoding="utf-8",
        )
        return self.output_path


def get_template_content(template_name: str = "report_template.html") -> str:
    """Get the content of a template file."""
    template_dir = Path(__file__).parent
    template_path = template_dir / template_name
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")
    return template_path.read_text(encoding="utf-8")


class HtmlReporter(BaseReporter):
    """Export diagnostic results to an interactive HTML report."""

    def __init__(
        self, output_path: Path | str, title: str = "Network Diagnostic Report"
    ):
        self.output_path = Path(output_path)
        self.title = title
        self.template = get_template_content()

    def export(self, report: dict[str, Any]) -> Path:
        """Export report to HTML file."""
        html_content = self._render_template(report)
        self.output_path.write_text(html_content, encoding="utf-8")
        return self.output_path

    def _render_template(self, report: dict[str, Any]) -> str:
        """Render the HTML template with report data."""
        metadata = report.get("metadata", {})
        results = report.get("results", [])
        all_passed = report.get("all_passed", False)

        # Calculate statistics
        total_checks = len(results)
        passed_checks = sum(1 for r in results if r.get("passed", False))
        failed_checks = total_checks - passed_checks
        skipped_checks = len([r for r in results if "Skipped" in r.get("report", "")])

        # Format timestamp
        timestamp = metadata.get("timestamp", datetime.now().isoformat())
        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            formatted_time = dt.strftime("%B %d, %Y at %I:%M:%S %p %Z")
        except Exception:
            formatted_time = timestamp

        # Determine summary status
        if all_passed:
            summary_icon = "✅"
            summary_status_class = "passed"
            summary_status_text = "All Checks Passed"
        else:
            summary_icon = "❌"
            summary_status_class = "failed"
            summary_status_text = "Some Checks Failed"

        # Auto-fix status
        auto_fix_status = (
            "✅ Enabled" if metadata.get("auto_fix", False) else "❌ Disabled"
        )

        # Generate checks HTML
        checks_html = self._generate_checks_html(results)

        # Render template with all variables
        return (
            self.template.replace("{{ title }}", self.title)
            .replace("{{ target_host }}", str(metadata.get("target_host", "N/A")))
            .replace("{{ target_port }}", str(metadata.get("target_port", "N/A")))
            .replace("{{ proxy_url }}", str(metadata.get("proxy_url", "N/A")))
            .replace("{{ platform }}", str(metadata.get("platform", "N/A")))
            .replace("{{ os_release }}", str(metadata.get("os_release", "")))
            .replace("{{ python_version }}", str(metadata.get("python_version", "N/A")))
            .replace("{{ auto_fix_status }}", auto_fix_status)
            .replace("{{ timestamp }}", formatted_time)
            .replace("{{ summary_icon }}", summary_icon)
            .replace("{{ summary_status_class }}", summary_status_class)
            .replace("{{ summary_status_text }}", summary_status_text)
            .replace("{{ total_checks }}", str(total_checks))
            .replace("{{ passed_checks }}", str(passed_checks))
            .replace("{{ failed_checks }}", str(failed_checks))
            .replace("{{ skipped_checks }}", str(skipped_checks))
            .replace("{{ checks_html }}", checks_html)
        )

    def _generate_checks_html(self, results: list) -> str:
        """Generate HTML for all checks."""
        html = []

        for result in results:
            passed = result.get("passed", False)
            section = result.get("section", "Unknown Check")
            report = result.get("report", "No report available")
            impact = result.get("impact_level", "MEDIUM").lower()
            recommendation = result.get("recommendation")
            bash_command = result.get("bash_command")
            fix_command = result.get("fix_command")
            timestamp = result.get("timestamp", "")

            # Determine status
            is_skipped = "Skipped" in report
            if is_skipped:
                status_class = "skipped"
                status_icon = "⏭️"
                status_text = "SKIPPED"
            elif passed:
                status_class = "passed"
                status_icon = "✅"
                status_text = "PASSED"
            else:
                status_class = "failed"
                status_icon = "❌"
                status_text = "FAILED"

            # Format timestamp
            try:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                formatted_ts = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                formatted_ts = timestamp

            # Build individual check HTML
            check_html = f"""
            <div class="check {status_class}" data-status="{status_class}">
                <div class="check-header" onclick="toggleCheck(this)">
                    <div class="left">
                        <span class="status-badge {status_class}">{status_icon} {status_text}</span>
                        <span class="section-name">{self._escape_html(section)}</span>
                    </div>
                    <span class="toggle-icon">▼</span>
                </div>
                <div class="check-body">
                    <div class="report">{self._escape_html(report)}</div>
                    <span class="impact {impact}">Impact: {impact.upper()}</span>
                    {self._generate_recommendation_html(recommendation)}
                    {self._generate_bash_command_html(bash_command)}
                    {self._generate_fix_command_html(fix_command)}
                    <div class="timestamp">⏱️ {formatted_ts}</div>
                </div>
            </div>
            """
            html.append(check_html)

        return "".join(html)

    def _generate_recommendation_html(self, recommendation: str | None) -> str:
        """Generate HTML for recommendation."""
        if not recommendation:
            return ""
        return f"""
        <div class="recommendation">
            <div class="label">💡 Recommendation</div>
            <div>{self._escape_html(recommendation)}</div>
        </div>
        """

    def _generate_bash_command_html(self, bash_command: str | None) -> str:
        """Generate HTML for bash command."""
        if not bash_command:
            return ""
        return f"""
        <div class="bash-command">
            <button class="copy-btn" onclick="copyCommand(this)">Copy</button>
            <code>{self._escape_html(bash_command)}</code>
        </div>
        """

    def _generate_fix_command_html(self, fix_command: str | None) -> str:
        """Generate HTML for fix command."""
        if not fix_command:
            return ""
        return f"""
        <div class="fix-command">
            <div class="label">🔧 Auto-Fix Command</div>
            <div class="bash-command fix">
                <button class="copy-btn" onclick="copyCommand(this)">Copy</button>
                <code>{self._escape_html(fix_command)}</code>
            </div>
        </div>
        """

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        if not text:
            return ""
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )
