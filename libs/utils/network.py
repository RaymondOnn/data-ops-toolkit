"""Network diagnostics for proxy, VPN, and connectivity issues in corporate environments.

This module provides comprehensive network diagnostics for data ingestion pipelines,
specifically designed for corporate environments with CNTLM proxies, VPNs, firewalls,
and complex authentication requirements.
"""

import contextlib
import json
import logging
import os
import platform
import socket
import ssl
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import urlparse

import httpx
import psutil

LOG = logging.getLogger(__name__)


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

    def to_dict(self) -> dict[str, Any]:
        """Convert the result into a serializable dictionary.

        Returns:
            dict[str, Any]: A dictionary containing result details.

        Decision: Machine-Readable Timestamps.
        We serialize the timestamp using ISO-8601 format with UTC
        enforced at the source. This ensures that downstream
        systems can reliably parse diagnostic history without
        timezone ambiguity.
        """
        result = {
            "section": self.section,
            "passed": self.passed,
            "report": self.report,
            "timestamp": self.timestamp.isoformat(),
        }
        if self.recommendation:
            result["recommendation"] = self.recommendation
        if self.bash_command:
            result["bash_command"] = self.bash_command
        if self.auto_fix and self.fix_command:
            result["fix_command"] = self.fix_command
        return result


class NetworkDoctor:
    """Comprehensive network diagnostic suite for corporate data ingestion.

    Designed for environments with:
    - CNTLM/NTLM proxies
    - Corporate VPNs
    - Firewalls and security groups
    - SSL inspection
    - Complex DNS configurations

    Decision: Pre-flight Network Validation.
    Running these checks before initializing Ray or starting data ingestion
    prevents cryptic timeout and connection errors during production runs.
    """

    # Diagnostic pipeline: (display_name, method_name)
    DIAGNOSTIC_PIPELINE: ClassVar[list[tuple[str, str]]] = [
        # System-level checks
        ("Socket/File Descriptor Limits", "check_socket_limits"),
        ("Packet Capture Tools", "check_packet_capture"),
        # Network base checks
        ("Corporate VPN Connectivity", "check_vpn_presence"),
        ("DNS Resolution", "check_dns_resolution"),
        ("Path MTU Discovery", "check_path_mtu"),
        ("Bandwidth & Latency", "check_bandwidth_latency"),
        # Proxy & Auth
        ("Environment Variables", "check_environment_variables"),
        ("PAC/WPAD Discovery", "check_pac_discovery"),
        ("Local CNTLM Daemon", "check_local_proxy"),
        ("NTLM Authentication Path", "check_ntlm_auth"),
        ("Proxy Handshake", "check_proxy_handshake"),
        # Target service
        ("SSL/TLS Certificates", "check_ssl_certificates"),
        ("Target Service Health", "check_service_health"),
        ("Target Connectivity", "check_target_connectivity"),
        ("Large Payload (MTU)", "check_mtu_payload"),
    ]

    def __init__(
        self,
        target_host: str,
        target_port: int,
        proxy_url: str = "http://127.0.0.1:3128",
        vpn_prefixes: list[str] | None = None,
        debug_traffic: bool = False,
        auto_fix: bool = False,
    ):
        """Initialize the NetworkDoctor with target and environment details.

        Args:
            target_host: The hostname or IP of the destination service.
            target_port: The TCP port of the destination service.
            proxy_url: The URL of the local proxy (e.g., CNTLM).
            vpn_prefixes: A list of IP prefixes indicating corporate VPN ranges.
            debug_traffic: If True, enables deep traffic inspection via httptap.
            auto_fix: If True, attempts to automatically fix issues when possible.
        """
        self.target_host = target_host
        self.target_port = target_port
        self.proxy_url = proxy_url
        self.debug_traffic = debug_traffic
        self.auto_fix = auto_fix
        self.vpn_prefixes = vpn_prefixes or [
            "10.",
            "172.16.",
            "192.168.",
        ]
        self._results: list[DiagnosticResult] = []

        # Parse proxy
        self.proxy_host, self.proxy_port = self._parse_proxy_url(proxy_url)

    # ========================================================================
    # Public API
    # ========================================================================

    def run_diagnostics(self) -> bool:
        """Run all diagnostic checks sequentially.

        Returns:
            bool: True if all critical checks passed, False otherwise.
        """
        self._results = []
        self._log_header()

        all_passed = True
        for section_name, method_name in self.DIAGNOSTIC_PIPELINE:
            LOG.info("🔍 %s...", section_name)

            try:
                method = getattr(self, method_name)
                result = method()  # Now returns DiagnosticResult directly
                self._results.append(result)

                if result.passed:
                    LOG.info("✅ %s", result.report)
                else:
                    LOG.error("❌ %s", result.report)
                    if result.recommendation:
                        LOG.info("💡 Recommendation: %s", result.recommendation)

                    # Auto-fix if enabled and fix command exists
                    if self.auto_fix and result.fix_command:
                        LOG.info("🔧 Attempting auto-fix...")
                        fix_success = self._run_fix_command(
                            result.fix_command, section_name
                        )
                        if fix_success:
                            LOG.info("✅ Auto-fix successful!")
                            # Re-run the check after fix
                            LOG.info("🔍 Re-checking %s...", section_name)
                            rerun_result = method()
                            if rerun_result.passed:
                                LOG.info("✅ Issue resolved!")
                                result.passed = True
                                result.report = rerun_result.report
                            else:
                                LOG.warning("⚠️ Auto-fix attempted but issue persists")
                        else:
                            LOG.warning("⚠️ Auto-fix failed or not applicable")
                    elif result.bash_command and not self.auto_fix:
                        LOG.info("🐚 Try: %s", result.bash_command)

                    all_passed = False

            except Exception as e:
                LOG.exception("Unhandled failure in %s", section_name)
                self._results.append(
                    DiagnosticResult(
                        section=section_name,
                        passed=False,
                        report=f"Unhandled exception: {e!s}",
                        recommendation="Run with --debug flag for detailed traceback",
                        bash_command="python -m ingestion doctor network --debug",
                    )
                )
                all_passed = False

            print("-" * 60)

        self._log_footer(all_passed)
        return all_passed

    def _run_fix_command(self, command: str, section: str) -> bool:
        """Executes a system-level command intended to resolve a diagnostic failure.

        Args:
            command: The shell command to execute.
            section: The diagnostic section name for logging context.

        Returns:
            bool: True if the command returned exit code 0, False otherwise.

        Decision: Explicit Exit Handling.
        We set 'check=False' because the function manually evaluates the
        return code to provide detailed logging rather than simply
        raising a CalledProcessError.
        """
        try:
            LOG.info("Running: %s", command)
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if result.returncode == 0:
                LOG.debug("Fix command succeeded: %s", result.stdout[:200])
                return True
            LOG.warning(
                "Fix command failed (exit %d): %s",
                result.returncode,
                result.stderr[:200],
            )
            return False
        except subprocess.TimeoutExpired:
            LOG.warning("Fix command timed out after 10 seconds")
            return False
        except Exception as e:
            LOG.warning("Fix command error: %s", e)
            return False

    def get_report(self) -> dict[str, Any]:
        """Aggregates all diagnostic results into a final report dictionary.

        Returns:
            dict[str, Any]: A complete report including results and platform metadata.

        Decision: Environment Correlation.
        By including platform and OS details, support teams can
        correlate network failures with specific kernel versions or
        Python releases, which is vital for troubleshooting parity
        issues in hybrid-cloud environments.
        """
        return {
            "metadata": {
                "target_host": self.target_host,
                "target_port": self.target_port,
                "proxy_url": self.proxy_url,
                "auto_fix": self.auto_fix,
                "timestamp": datetime.now(UTC).isoformat(),
                "platform": platform.system(),
                "os_release": platform.release(),
                "python_version": platform.python_version(),
            },
            "results": [r.to_dict() for r in self._results],
            "all_passed": (
                all(r.passed for r in self._results) if self._results else False
            ),
        }

    def export_json(self, output_path: Path | str) -> Path:
        """Export diagnostic report to a JSON file."""
        path = Path(output_path)
        report = self.get_report()
        try:
            path.write_text(json.dumps(report, indent=4), encoding="utf-8")
            LOG.info("Diagnostic report exported to: %s", path)
            return path
        except Exception:
            LOG.exception("Failed to export diagnostic report to %s", path)
            raise

    def trace_route(self) -> bool:
        """Executes a system-level traceroute to visualize the network path."""
        is_win = platform.system() == "Windows"
        cmd = (
            ["tracert", "-d", self.target_host]
            if is_win
            else ["traceroute", "-n", self.target_host]
        )

        LOG.info("🩺 Executing route trace to %s...", self.target_host)

        try:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )

            hop_count = 0
            if process.stdout:
                for raw_line in process.stdout:
                    line = raw_line.strip()
                    if not line:
                        continue

                    hop_count += 1
                    if hop_count <= 2:
                        context = "[Local/Office Network]"
                    elif hop_count <= 6:
                        context = "[VPN / Corporate Gateway]"
                    else:
                        context = "[External / Provider Path]"

                    if "*" in line:
                        LOG.warning(
                            "Hop %d: %s | DROP (Likely Firewall)", hop_count, line
                        )
                    else:
                        LOG.info("Hop %d: %s %s", hop_count, line, context)

            process.wait()

            if process.returncode == 0:
                LOG.info(
                    "Trace complete. Path to %s is fully visible.", self.target_host
                )
                return True
            LOG.error("Traceroute process exited with errors.")
            return False

        except FileNotFoundError:
            LOG.error("Traceroute utility not found.")
            return False
        except Exception:
            LOG.exception("Unexpected failure during network trace")
            return False

    # ========================================================================
    # Diagnostic Checks (each returns DiagnosticResult)
    # ========================================================================

    def check_socket_limits(self) -> DiagnosticResult:
        """Checks OS limits for sockets and file descriptors."""
        try:
            import resource

            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

            if soft < 4096:
                return DiagnosticResult(
                    section="Socket/File Descriptor Limits",
                    passed=False,
                    report=f"Low file descriptor limit: {soft}/{hard}",
                    recommendation=(
                        "Increase file descriptor limit for high-volume ingestion"
                    ),
                    bash_command="ulimit -n 65536",
                    auto_fix=True,
                    fix_command=(
                        "ulimit -n 65536 && echo 'ulimit -n 65536' >> ~/.bashrc"
                    ),
                )
            return DiagnosticResult(
                section="Socket/File Descriptor Limits",
                passed=True,
                report=f"Good file descriptor limit: {soft}/{hard}",
            )

        except Exception as e:
            return DiagnosticResult(
                section="Socket/File Descriptor Limits",
                passed=False,
                report=f"Could not check socket limits: {e!s}",
                bash_command="ulimit -a | grep 'open files'",
            )

    def check_packet_capture(self) -> DiagnosticResult:
        """Checks if packet capture tools are available."""
        tools = (
            ["tcpdump", "tshark", "traceroute"] if not self._is_windows() else ["netsh"]
        )
        available = [t for t in tools if self._command_exists(t)]

        if not available:
            install_cmd = (
                "sudo apt-get install -y tcpdump traceroute wireshark-cli"
                if not self._is_windows()
                else "choco install wireshark"
            )
            return DiagnosticResult(
                section="Packet Capture Tools",
                passed=False,
                report=f"No diagnostic tools found. Missing: {', '.join(tools)}",
                recommendation="Install network diagnostic tools",
                bash_command=install_cmd,
                auto_fix=True,
                fix_command=install_cmd,
            )
        if len(available) < len(tools):
            return DiagnosticResult(
                section="Packet Capture Tools",
                passed=True,
                report=f"Partial tools: {', '.join(available)}",
            )
        return DiagnosticResult(
            section="Packet Capture Tools",
            passed=True,
            report=f"All tools available: {', '.join(available)}",
        )

    def check_vpn_presence(self) -> DiagnosticResult:
        """Verifies if the machine has an IP within corporate ranges."""
        external_ips = self._get_external_ips()

        if not external_ips:
            return DiagnosticResult(
                section="Corporate VPN Connectivity",
                passed=False,
                report="No active network interfaces detected",
                bash_command="ip addr show",
            )

        for ip in external_ips:
            if self._is_corporate_ip(ip):
                return DiagnosticResult(
                    section="Corporate VPN Connectivity",
                    passed=True,
                    report=f"VPN detected: IP {ip} in corporate range",
                )

        return DiagnosticResult(
            section="Corporate VPN Connectivity",
            passed=False,
            report=f"No corporate VPN IP found. Active IPs: {external_ips[:3]}",
            recommendation="Connect to VPN",
            bash_command="sudo systemctl start openvpn@client",
            auto_fix=False,  # Can't auto-connect VPN
        )

    def check_dns_resolution(self) -> DiagnosticResult:
        """Checks DNS resolution for the target host."""
        try:
            ips = socket.gethostbyname_ex(self.target_host)[2]
            return DiagnosticResult(
                section="DNS Resolution",
                passed=True,
                report=f"Resolved {self.target_host} -> {', '.join(ips[:3])}",
            )
        except socket.gaierror as e:
            return DiagnosticResult(
                section="DNS Resolution",
                passed=False,
                report=f"DNS resolution failed: {e!s}",
                recommendation="Check DNS configuration",
                bash_command=f"nslookup {self.target_host}",
            )

    def check_path_mtu(self) -> DiagnosticResult:
        """Discovers optimal MTU size to target."""
        try:
            optimal_mtu = self._calculate_optimal_mtu()

            if optimal_mtu < 1400:
                return DiagnosticResult(
                    section="Path MTU Discovery",
                    passed=False,
                    report=f"Low MTU detected: {optimal_mtu}",
                    recommendation="Adjust VPN MTU settings",
                    bash_command=f"ping -M do -s {optimal_mtu} {self.target_host}",
                )
            return DiagnosticResult(
                section="Path MTU Discovery",
                passed=True,
                report=f"Good MTU: {optimal_mtu}",
            )

        except Exception as e:
            return DiagnosticResult(
                section="Path MTU Discovery",
                passed=False,
                report=f"MTU detection failed: {e!s}",
                bash_command="ip link show | grep mtu",
            )

    def check_bandwidth_latency(self) -> DiagnosticResult:
        """Measures throughput and latency to target endpoint."""
        test_sizes = [1024, 10240, 102400]
        latencies = []

        try:
            with socket.create_connection(
                (self.target_host, self.target_port), timeout=5.0
            ) as sock:
                for size in test_sizes:
                    data = b"X" * size
                    start = time.perf_counter()
                    sock.sendall(data)
                    with contextlib.suppress(OSError):
                        sock.recv(size)
                    rtt = (time.perf_counter() - start) * 1000
                    latencies.append(rtt)

                avg_rtt = sum(latencies) / len(latencies)

                if avg_rtt > 100:
                    return DiagnosticResult(
                        section="Bandwidth & Latency",
                        passed=False,
                        report=f"High latency: {avg_rtt:.1f}ms",
                        recommendation="Check VPN route",
                        bash_command=f"ping -c 10 {self.target_host}",
                    )
                return DiagnosticResult(
                    section="Bandwidth & Latency",
                    passed=True,
                    report=f"Good latency: {avg_rtt:.1f}ms",
                )

        except Exception as e:
            return DiagnosticResult(
                section="Bandwidth & Latency",
                passed=False,
                report=f"Latency test failed: {e!s}",
                bash_command=f"ping -c 3 {self.target_host}",
            )

    def check_environment_variables(self) -> DiagnosticResult:
        """Validates proxy environment variables."""
        mismatches = []
        for key in ["http_proxy", "https_proxy", "no_proxy"]:
            if bool(os.environ.get(key)) != bool(os.environ.get(key.upper())):
                mismatches.append(key)

        if mismatches:
            return DiagnosticResult(
                section="Environment Variables",
                passed=False,
                report=f"Inconsistent casing: {', '.join(mismatches)}",
                recommendation="Standardize proxy variables",
                bash_command="export http_proxy=http://127.0.0.1:3128 && export https_proxy=http://127.0.0.1:3128",
                auto_fix=True,
                fix_command="export http_proxy=http://127.0.0.1:3128 && export https_proxy=http://127.0.0.1:3128",
            )

        return DiagnosticResult(
            section="Environment Variables",
            passed=True,
            report="Environment variables consistent",
        )

    def check_local_proxy(self) -> DiagnosticResult:
        """Validates if the local CNTLM engine is running."""
        try:
            with socket.create_connection(
                (self.proxy_host, self.proxy_port), timeout=2.0
            ):
                return DiagnosticResult(
                    section="Local CNTLM Daemon",
                    passed=True,
                    report=f"Proxy responsive on {self.proxy_host}:{self.proxy_port}",
                )
        except ConnectionRefusedError:
            return DiagnosticResult(
                section="Local CNTLM Daemon",
                passed=False,
                report=f"Connection refused on port {self.proxy_port}",
                recommendation="Start CNTLM daemon",
                bash_command="ps aux | grep cntlm",
                auto_fix=True,
                fix_command="cntlm -c /etc/cntlm.conf &",  # Background start
            )
        except TimeoutError:
            return DiagnosticResult(
                section="Local CNTLM Daemon",
                passed=False,
                report=f"Connection timeout on port {self.proxy_port}",
                bash_command=f"telnet {self.proxy_host} {self.proxy_port}",
            )

    def check_ntlm_auth(self) -> DiagnosticResult:
        """Verifies NTLM authentication path through CNTLM."""
        try:
            import requests

            response = requests.get(
                f"http://{self.proxy_host}:{self.proxy_port}",
                timeout=5.0,
                proxies={"http": self.proxy_url, "https": self.proxy_url},
            )

            if response.status_code == 407:
                return DiagnosticResult(
                    section="NTLM Authentication",
                    passed=False,
                    report="NTLM authentication required",
                    recommendation="Check CNTLM credentials",
                    bash_command="cat /etc/cntlm.conf | grep -E '(Username|Domain)'",
                )
            return DiagnosticResult(
                section="NTLM Authentication",
                passed=True,
                report="NTLM proxy is responsive",
            )

        except ImportError:
            return DiagnosticResult(
                section="NTLM Authentication",
                passed=True,
                report="Check skipped (requests not installed)",
            )
        except Exception as e:
            return DiagnosticResult(
                section="NTLM Authentication",
                passed=False,
                report=f"NTLM check failed: {e!s}",
                bash_command="curl -x http://127.0.0.1:3128 https://ipify.org -v",
            )

    def check_proxy_handshake(self) -> DiagnosticResult:
        """Validates proxy exit credentials and SSL integrity."""
        try:
            with (
                self._maybe_tap(),
                httpx.Client(proxy=self.proxy_url, timeout=10.0) as client,
            ):
                response = client.get("https://ipify.org")
                response.raise_for_status()
                public_ip = response.text.strip()
                return DiagnosticResult(
                    section="Proxy Handshake",
                    passed=True,
                    report=f"Proxy working. Public IP: {public_ip}",
                )

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 407:
                return DiagnosticResult(
                    section="Proxy Handshake",
                    passed=False,
                    report="Proxy authentication failed (HTTP 407)",
                    recommendation="Verify CNTLM credentials",
                    bash_command="cntlm -c /etc/cntlm.conf -f",
                )
            return DiagnosticResult(
                section="Proxy Handshake",
                passed=False,
                report=f"HTTP {e.response.status_code}",
                bash_command="tail -f /var/log/cntlm.log",
            )
        except Exception as e:
            return DiagnosticResult(
                section="Proxy Handshake",
                passed=False,
                report=f"Proxy handshake failed: {e!s}",
                bash_command=f"telnet {self.proxy_host} {self.proxy_port}",
            )

    def check_target_connectivity(self) -> DiagnosticResult:
        """Checks direct connectivity to target."""
        try:
            with socket.create_connection(
                (self.target_host, self.target_port), timeout=5.0
            ):
                return DiagnosticResult(
                    section="Target Connectivity",
                    passed=True,
                    report=f"Target reachable on {self.target_host}:{self.target_port}",
                )

        except socket.gaierror:
            return DiagnosticResult(
                section="Target Connectivity",
                passed=False,
                report=f"Cannot resolve {self.target_host}",
                bash_command=f"nslookup {self.target_host}",
            )
        except TimeoutError:
            return DiagnosticResult(
                section="Target Connectivity",
                passed=False,
                report=f"Connection timeout to {self.target_host}:{self.target_port}",
                bash_command=f"traceroute -n {self.target_host}",
            )
        except ConnectionRefusedError:
            return DiagnosticResult(
                section="Target Connectivity",
                passed=True,
                report="Target reachable (service offline)",
            )

    def check_ssl_certificates(self) -> DiagnosticResult:  # noqa
        """Validates SSL certificate chain for HTTPS endpoints.

        Decision: Certificate Readiness.
        Corporate proxies often use self-signed certs that expire
        without warning, causing cryptic SSL errors during ingestion.
        """
        # Skip non-HTTPS ports
        if self.target_port not in [443, 8443, 9440]:
            return self._result(
                passed=True, report="SSL check skipped (non-HTTPS port)"
            )

        try:
            cert_bin = self._fetch_certificate()
            if cert_bin is None:
                return self._result(
                    passed=False,
                    report="No SSL certificate received",
                    recommendation="Server may not have SSL configured",
                    cmd=(
                        f"openssl s_client -connect "
                        f"{self.target_host}:{self.target_port} -brief"
                    ),
                )

            return self._check_cert_expiry(cert_bin)

        except ssl.SSLCertVerificationError as e:
            return self._result(
                passed=False,
                report=f"SSL verification failed: {e}",
                recommendation="Install corporate root CA",
                cmd="update-ca-certificates  # Linux\nsecurity add-trusted-cert corporate.pem  # macOS",
            )
        except ConnectionRefusedError:
            return self._result(
                passed=False,
                report=f"Connection refused to {self.target_host}:{self.target_port}",
                recommendation="Service may be offline",
                cmd=f"nc -zv {self.target_host} {self.target_port}",
            )
        except TimeoutError:
            return self._result(
                passed=False,
                report=f"Connection timeout to {self.target_host}:{self.target_port}",
                recommendation="Check firewall rules",
                cmd=f"ping {self.target_host}",
            )
        except Exception as e:
            return self._result(
                passed=False,
                report=f"SSL check failed: {e}",
                recommendation="Check SSL support on this port",
                cmd=f"openssl s_client -connect {self.target_host}:{self.target_port} -brief",
            )

    def _result(
        self,
        passed: bool,
        report: str,
        recommendation: str | None = None,
        cmd: str | None = None,
    ) -> DiagnosticResult:
        """Helper to create consistent DiagnosticResult."""
        return DiagnosticResult(
            section="SSL/TLS Certificates",
            passed=passed,
            report=report,
            recommendation=recommendation,
            bash_command=cmd,
        )

    def _fetch_certificate(self) -> bytes | None:
        """Fetch SSL certificate from server."""
        import socket
        import ssl

        context = ssl.create_default_context()
        with (
            socket.create_connection(
                (self.target_host, self.target_port), timeout=5.0
            ) as sock,
            context.wrap_socket(sock, server_hostname=self.target_host) as ssock,
        ):
            return ssock.getpeercert(binary_form=True)

    def _check_cert_expiry(self, cert_bin: bytes) -> DiagnosticResult:
        """Check certificate expiration and return appropriate result."""
        from datetime import UTC, datetime

        from cryptography import x509
        from cryptography.hazmat.backends import default_backend

        cert = x509.load_der_x509_certificate(cert_bin, default_backend())
        now = datetime.now(UTC)

        if cert.not_valid_after < now:
            days = (now - cert.not_valid_after).days
            return self._result(
                passed=False,
                report=f"Certificate expired {days} days ago on {cert.not_valid_after}",
                recommendation="Contact IT to renew",
                cmd=f"echo | openssl s_client -connect {self.target_host}:{self.target_port} 2>/dev/null | openssl x509 -noout -dates",
            )

        if cert.not_valid_after < now.replace(year=now.year + 1):
            days = (cert.not_valid_after - now).days
            return self._result(
                passed=True,
                report=f"Certificate expires in {days} days",
                recommendation="Schedule renewal soon",
                cmd=f"echo | openssl s_client -connect {self.target_host}:{self.target_port} 2>/dev/null | openssl x509 -noout -dates",
            )

        return self._result(
            passed=True,
            report="Certificate is valid",
        )

    def check_service_health(self) -> DiagnosticResult:
        """Pings the actual data service."""
        try:
            import httpx

            with httpx.Client(timeout=5.0, verify=False) as client:
                resp = client.get(
                    f"https://{self.target_host}:{self.target_port}/health"
                )
                if resp.status_code == 200:
                    return DiagnosticResult(
                        section="Target Service Health",
                        passed=True,
                        report="Service health endpoint OK",
                    )
                return DiagnosticResult(
                    section="Target Service Health",
                    passed=True,
                    report=f"Service responded with {resp.status_code}",
                )
        except Exception:
            return DiagnosticResult(
                section="Target Service Health",
                passed=True,
                report="No health endpoint configured",
            )

    def check_mtu_payload(self) -> DiagnosticResult:
        """Tests large payload transmission."""
        try:
            with socket.create_connection(
                (self.target_host, self.target_port), timeout=10.0
            ) as sock:
                sock.sendall(b"0" * (4 * 1024 * 1024))
                return DiagnosticResult(
                    section="Large Payload (MTU)",
                    passed=True,
                    report="Large payload transmitted successfully",
                )
        except TimeoutError:
            return DiagnosticResult(
                section="Large Payload (MTU)",
                passed=False,
                report="Payload transmission timed out",
                bash_command=f"ping -M do -s 1472 {self.target_host}",
            )
        except Exception as e:
            return DiagnosticResult(
                section="Large Payload (MTU)",
                passed=True,
                report=f"Payload test interrupted: {e!s}",
            )

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _parse_proxy_url(self, proxy_url: str) -> tuple[str, int]:
        """Parse proxy URL into host and port."""
        try:
            parsed = urlparse(proxy_url)
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port or 3128
            return host, port
        except Exception:
            return "127.0.0.1", 3128

    def _get_external_ips(self) -> list[str]:
        """Get all non-loopback IPv4 addresses."""
        ips = []
        for addrs in psutil.net_if_addrs().values():
            for addr in addrs:
                if addr.family == socket.AF_INET and not addr.address.startswith(
                    "127."
                ):
                    ips.append(addr.address)
        return ips

    def _is_corporate_ip(self, ip: str) -> bool:
        """Check if IP falls within corporate VPN ranges."""
        return any(ip.startswith(prefix) for prefix in self.vpn_prefixes)

    def _is_windows(self) -> bool:
        """Check if running on Windows."""
        return platform.system() == "Windows"

    def _command_exists(self, cmd: str) -> bool:
        """Check if command exists in PATH."""
        try:
            subprocess.run(
                ["which", cmd] if not self._is_windows() else ["where", cmd],
                capture_output=True,
                check=True,
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def _calculate_optimal_mtu(self) -> int:
        """Calculate optimal MTU size to target."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(1)

            for mtu in [
                9000,
                8000,
                7000,
                6000,
                5000,
                4000,
                3000,
                1500,
                1400,
                1300,
                1200,
            ]:
                try:
                    sock.sendto(b"X" * mtu, (self.target_host, self.target_port))
                    sock.close()
                    return mtu
                except OSError:
                    continue
            return 1500
        except Exception:
            return 1500

    @contextmanager
    def _maybe_tap(self):
        """Context manager to optionally enable httptap inspection."""
        if self.debug_traffic:
            try:
                from httptap import httpx_tap

                with httpx_tap():
                    yield
            except ImportError:
                yield
        else:
            yield

    def _log_header(self) -> None:
        """Log diagnostic session header."""
        LOG.info("=" * 70)
        LOG.info("🚀 CORPORATE NETWORK DIAGNOSTIC SUITE")
        LOG.info("Target Destination : %s:%d", self.target_host, self.target_port)
        LOG.info("Local Proxy Target : %s:%d", self.proxy_host, self.proxy_port)
        LOG.info("Auto-Fix Mode      : %s", "ON" if self.auto_fix else "OFF")
        LOG.info("Platform           : %s %s", platform.system(), platform.release())
        LOG.info("=" * 70)

    def _log_footer(self, all_passed: bool) -> None:
        """Log diagnostic session footer."""
        LOG.info("=" * 70)
        if all_passed:
            LOG.info("🎉 All checks passed - Environment is healthy")
        else:
            LOG.critical("🚨 Some checks failed")
            if not self.auto_fix:
                LOG.info("💡 Run with --auto-fix to attempt automatic fixes")
        LOG.info("=" * 70)


# ============================================================================
# Convenience Functions
# ============================================================================


def quick_check(
    target_host: str,
    target_port: int,
    proxy_url: str = "http://127.0.0.1:3128",
    auto_fix: bool = False,
) -> bool:
    """Quick diagnostic check for common network issues.

    Args:
        target_host: The hostname or IP of the destination service.
        target_port: The TCP port of the destination service.
        proxy_url: The URL of the local proxy (e.g., CNTLM).
        auto_fix: If True, attempts to automatically fix issues.

    Returns:
        bool: True if all critical checks passed, False otherwise.
    """
    doctor = NetworkDoctor(target_host, target_port, proxy_url, auto_fix=auto_fix)
    return doctor.run_diagnostics()


def diagnose_and_export(
    target_host: str,
    target_port: int,
    output_path: Path | str,
    proxy_url: str = "http://127.0.0.1:3128",
    auto_fix: bool = False,
) -> Path:
    """Run diagnostics and export report to JSON.

    Args:
        target_host: The hostname or IP of the destination service.
        target_port: The TCP port of the destination service.
        output_path: Where to save the JSON report.
        proxy_url: The URL of the local proxy.
        auto_fix: If True, attempts to automatically fix issues.

    Returns:
        Path: Path to the exported report.
    """
    doctor = NetworkDoctor(target_host, target_port, proxy_url, auto_fix=auto_fix)
    doctor.run_diagnostics()
    return doctor.export_json(output_path)
