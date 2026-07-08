"""System-level diagnostic checks."""

from libs.network.checks.base import BaseCheck
from libs.network.core.result import DiagnosticResult
from libs.network.utils.platform import PlatformUtils


class SystemCheck(BaseCheck):
    """System-level checks for socket limits and tools."""

    def get_section_name(self) -> str:
        return "System"

    def run(self) -> DiagnosticResult:
        """Run all system checks."""
        # For now, just run socket limits as the main check
        # Individual methods can be called separately or we can aggregate
        return self.check_socket_limits()

    def check_socket_limits(self):
        """Check OS limits for sockets and file descriptors."""
        try:
            import resource

            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

            if soft < 4096:
                return self._failure(
                    report=f"Low file descriptor limit: {soft}/{hard}",
                    recommendation="Increase file descriptor limit for high-volume ingestion",
                    bash_command="ulimit -n 65536",
                    auto_fix=True,
                    fix_command="ulimit -n 65536 && echo 'ulimit -n 65536' >> ~/.bashrc",
                    impact_level="HIGH",
                )
            return self._success(f"Good file descriptor limit: {soft}/{hard}")

        except Exception as e:
            return self._failure(
                report=f"Could not check socket limits: {e!s}",
                bash_command="ulimit -a | grep 'open files'",
                impact_level="LOW",
            )

    def check_packet_capture(self):
        """Check if packet capture tools are available."""
        tools = (
            ["tcpdump", "tshark", "traceroute"]
            if not PlatformUtils.is_windows()
            else ["netsh"]
        )
        available = [t for t in tools if PlatformUtils.command_exists(t)]

        if not available:
            install_cmd = (
                "sudo apt-get install -y tcpdump traceroute wireshark-cli"
                if not PlatformUtils.is_windows()
                else "choco install wireshark"
            )
            return self._failure(
                report=f"No diagnostic tools found. Missing: {', '.join(tools)}",
                recommendation="Install network diagnostic tools",
                bash_command=install_cmd,
                auto_fix=True,
                fix_command=install_cmd,
                impact_level="MEDIUM",
            )
        if len(available) < len(tools):
            return self._success(f"Partial tools: {', '.join(available)}")
        return self._success(f"All tools available: {', '.join(available)}")
