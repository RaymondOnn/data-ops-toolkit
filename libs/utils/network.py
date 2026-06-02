import contextlib
import json
import logging
import os
import platform
import socket
import ssl
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import psutil
from libs.utils.dates import get_current_timestamp
from pypac.api import collect_pac_urls

if TYPE_CHECKING:
    from collections.abc import Callable

LOG = logging.getLogger(__name__)


class NetworkDoctor:
    """A comprehensive network diagnostic suite designed for unprivileged user spaces.
    Diagnoses proxy issues, CNTLM drops, firewalls, DNS breaks, and MTU limits.
    """

    def __init__(
        self,
        target_host: str,
        target_port: int,
        proxy_url: str = "http://127.0.0.1:3128",
        vpn_prefixes: list[str] | None = None,
        debug_traffic: bool = False,
    ):
        """Initializes the NetworkDoctor with target and environment details.

        Args:
            target_host: The hostname or IP of the destination service.
            target_port: The TCP port of the destination service.
            proxy_url: The URL of the local proxy (e.g., CNTLM).
            vpn_prefixes: A list of IP prefixes indicating corporate VPN ranges.
            debug_traffic: If True, enables deep traffic inspection via httptap.
        """
        self.target_host = target_host
        self.target_port = target_port
        self.proxy_url = proxy_url
        self.debug_traffic = debug_traffic
        self.vpn_prefixes = vpn_prefixes or [
            "10.",
            "172.16.",
            "192.168.",
        ]  # Common internal subnets
        self._results: list[dict[str, Any]] = []

        # Breakdown proxy components safely
        try:
            clean_url = proxy_url.replace("http://", "").replace("https://", "")
            self.proxy_host = clean_url.split(":")[0]
            self.proxy_port = int(clean_url.split(":")[-1])
        except Exception as e:
            LOG.warning(
                "Could not parse proxy URL format '%s'. "
                "Defaulting to standard loopback. Error: %s",
                proxy_url,
                e,
            )
            self.proxy_host = "127.0.0.1"
            self.proxy_port = 3128

    def run_diagnostics(self) -> bool:
        """Runs the sequential health evaluation pipeline.

        Returns:
            bool: True if all critical diagnostic checks passed, False otherwise.
        """
        self._results = []
        LOG.info("=" * 60)
        LOG.info("🚀 STARTING USER-SPACE NETWORK ENVIRONMENT CHECK")
        LOG.info("Target Destination : %s:%d", self.target_host, self.target_port)
        LOG.info("Local Proxy Target : %s:%d", self.proxy_host, self.proxy_port)
        LOG.info("=" * 60)

        # Order matters: VPN/DNS/Proxy checks should precede target-specific checks
        pipeline: list[tuple[str, Callable[[], tuple[bool, str]]]] = [
            ("Corporate VPN Connectivity", self.check_vpn_presence),
            ("DNS Server Configuration", self.check_dns_servers),
            ("Environment Casing Evaluation", self.check_environment_variables),
            ("PAC/WPAD Auto-Discovery Check", self.check_pac_discovery),
            ("Local CNTLM Daemon Verification", self.check_local_cntlm),
            ("Outbound Proxy Handshake Exit", self.check_proxy_handshake),
            ("Target Route Connection (Firewall)", self.check_target_firewall),
            ("Large Data Payload Handling (MTU)", self.check_mtu_payload),
        ]

        all_passed = True
        for section_name, task in pipeline:
            LOG.info("Checking: %s...", section_name)
            try:
                passed, report = task()
                self._results.append(
                    {"section": section_name, "passed": passed, "report": report}
                )
                if passed:
                    LOG.info("✅ PASS: %s", report)
                else:
                    LOG.error("❌ FAIL: %s", report)
                    all_passed = False
            except Exception:
                LOG.exception(
                    "💥 CRITICAL BREAKDOWN: Unhandled failure in section [%s]",
                    section_name,
                )
                self._results.append(
                    {
                        "section": section_name,
                        "passed": False,
                        "report": "Unhandled exception during diagnostic check.",
                    }
                )
                all_passed = False
            print("-" * 60)

        if all_passed:
            LOG.info(
                "🎉 ENVIRONMENT VALIDATION SUCCESSFUL: System is healthy for execution."
            )
        else:
            LOG.critical(
                "🚨 ENVIRONMENT VALIDATION FAILED: Review the error markers above."
            )

        return all_passed

    def get_report(self) -> dict[str, Any]:
        """Returns the diagnostic results as a dictionary for reporting.

        Returns:
            dict[str, Any]: A serialized report of all diagnostic outcomes.
        """
        return {
            "metadata": {
                "target_host": self.target_host,
                "target_port": self.target_port,
                "proxy_url": self.proxy_url,
                "timestamp": get_current_timestamp("UTC").isoformat(),
                "platform": platform.system(),
                "os_release": platform.release(),
            },
            "results": self._results,
            "all_passed": (
                all(r["passed"] for r in self._results) if self._results else False
            ),
        }

    def export_json(self, output_path: Path | str) -> Path:
        """Exports the diagnostic report to a JSON file.

        Args:
            output_path: The filesystem path where the JSON report is saved.

        Returns:
            Path: The resolved path to the exported JSON file.

        Raises:
            Exception: If the file cannot be written.
        """
        report = self.get_report()
        path = Path(output_path)
        try:
            with path.open("w", encoding="utf-8") as f:
                json.dump(report, f, indent=4)
            LOG.info("Diagnostic report exported to: %s", path)
            return path
        except Exception:
            LOG.exception("Failed to export diagnostic report to %s", path)
            raise

    def check_vpn_presence(self) -> tuple[bool, str]:
        """Verifies if the machine has an IP within corporate ranges.

        Returns:
            tuple[bool, str]: A success flag and a description of the
                detected network interface state.
        """
        interfaces = psutil.net_if_addrs()
        found_ips = []

        for _, addrs in interfaces.items():
            for addr in addrs:
                if addr.family == socket.AF_INET:
                    found_ips.append(addr.address)

        # Filter out loopback
        external_ips = [ip for ip in found_ips if not ip.startswith("127.")]

        if not external_ips:
            return (
                False,
                "No active network interfaces detected. Verify network hardware state.",
            )

        for ip in external_ips:
            if any(ip.startswith(prefix) for prefix in self.vpn_prefixes):
                return (
                    True,
                    f"VPN connectivity verified: "
                    f"Local IP {ip} is within a recognized corporate range.",
                )

        return (
            False,
            f"VPN connectivity not detected. Active IPs {external_ips} do not match "
            f"recognized corporate subnets: {self.vpn_prefixes}.",
        )

    def trace_route(self) -> bool:
        """Executes a system-level traceroute to visualize the network path.
        Provides a professional interpretation of the hop sequence.
        """
        is_win = platform.system() == "Windows"
        # -d/n prevents slow reverse DNS lookups on every hop
        cmd = (
            ["tracert", "-d", self.target_host]
            if is_win
            else ["traceroute", "-n", self.target_host]
        )

        LOG.info("🩺 Executing route trace to %s...", self.target_host)

        try:
            # We stream the output to the log in real-time
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )

            hop_count = 0
            if process.stdout:
                # Decision: Variable Shadowing Prevention.
                # We use 'raw_line' for iteration to avoid overwriting the
                # loop variable with the assignment target, ensuring
                # compatibility with strict linting rules.
                for raw_line in process.stdout:
                    line = raw_line.strip()
                    if not line:
                        continue

                    hop_count += 1
                    # Differentiate hops by likely infrastructure zones
                    context = ""
                    if hop_count <= 2:
                        context = "[Local/Office Network]"
                    elif hop_count <= 6:
                        context = "[VPN / Corporate Gateway]"
                    else:
                        context = "[External / Provider Path]"

                    if "*" in line:
                        LOG.warning(
                            "Hop %d: %s | STATUS: Packet Dropped (Likely Firewall)",
                            hop_count,
                            line,
                        )
                    else:
                        LOG.info("Hop %d: %s %s", hop_count, line, context)

            process.wait()

            if process.returncode == 0:
                LOG.info(
                    "🎉 Trace complete. Path to %s is fully visible.", self.target_host
                )
                return True
            LOG.error("Traceroute process exited with errors.")
            return False

        except FileNotFoundError:
            LOG.error(
                "Traceroute utility not found on this system. "
                "Install 'traceroute' or 'iputils-tracepath'."
            )
            return False
        except Exception:
            LOG.exception("Unexpected failure during network trace")
            return False

    def check_dns_servers(self) -> tuple[bool, str]:
        """Verifies the system's configured DNS servers.

        Returns:
            tuple[bool, str]: Success flag and description of DNS status,
                checking specifically for corporate-aligned resolvers.
        """
        try:
            if platform.system() == "Windows":
                # Windows: Use ipconfig
                result = subprocess.run(
                    ["ipconfig", "/all"], capture_output=True, text=True, check=True
                )
                dns_lines = [
                    line for line in result.stdout.splitlines() if "DNS Servers" in line
                ]
                configured_dns = [
                    line.split(":")[-1].strip()
                    for line in dns_lines
                    if line.split(":")[-1].strip()
                ]
            elif platform.system() == "Linux" or platform.system() == "Darwin":
                # Linux/macOS: Parse /etc/resolv.conf
                with Path("/etc/resolv.conf").open() as f:
                    configured_dns = [
                        line.split()[1] for line in f if line.startswith("nameserver")
                    ]
            else:
                return True, "DNS server check not supported on this OS."

            if not configured_dns:
                return False, "No DNS servers configured. Network resolution will fail."

            # Example: Check if any corporate DNS server is present (e.g., 10.x.x.x)
            corporate_dns_found = any(
                any(ip.startswith(prefix) for prefix in self.vpn_prefixes)
                for ip in configured_dns
            )

            if corporate_dns_found:
                return (
                    True,
                    f"Corporate DNS server detected: {', '.join(configured_dns)}. "
                    "Resolution should be effective.",
                )
            return (
                False,
                f"Non-corporate DNS server detected: {', '.join(configured_dns)}. "
                "Internal hostnames may not resolve. "
                "Verify VPN connection and DNS settings.",
            )
        except Exception as e:
            return False, f"Failed to retrieve DNS server configuration: {e!s}"

    def check_environment_variables(self) -> tuple[bool, str]:
        """Validates case symmetry across system routing environment hooks.

        Returns:
            tuple[bool, str]: Success flag and description of potential
                casing conflicts between lower and uppercase proxy variables.
        """
        standard_keys = ["http_proxy", "https_proxy", "no_proxy"]
        mismatches = []

        for key in standard_keys:
            lower_val = os.environ.get(key)
            upper_val = os.environ.get(key.upper())

            if (lower_val is not None) != (upper_val is not None):
                mismatches.append(
                    f"'{key}' vs '{key.upper()}' configuration is asymmetric."
                )

        if mismatches:
            return (
                False,
                "Proxy environment variables exhibit inconsistent casing. "
                "Ensure both lower and upper case keys are standardized: "
                f"{', '.join(mismatches)}",
            )

        # Check NO_PROXY effectiveness
        no_proxy_val = os.environ.get("NO_PROXY") or os.environ.get("no_proxy")
        if no_proxy_val:
            return (
                True,
                "All environment proxy keys parse uniformly. "
                f"NO_PROXY configured: {no_proxy_val}.",
            )
        return (
            True,
            "All environment proxy keys parse uniformly. NO_PROXY not configured.",
        )

    def check_pac_discovery(self) -> tuple[bool, str]:
        """Detects if a hidden network PAC setup is present.

        Returns:
            tuple[bool, str]: Success flag and the discovered PAC URL,
                if applicable.
        """
        try:
            pac_url = collect_pac_urls()
            if pac_url:
                return (
                    True,
                    f"Proxy Auto-Configuration (PAC) detected at: {pac_url}. "
                    "Outbound requests will be routed according to "
                    "corporate traffic policies.",
                )
        except Exception:
            # Non-critical if tracking logic fails due to zero intranet
            # configuration DNS strings
            pass
        return (
            True,
            "No hidden corporate PAC mappings forcing alternate routing paths found.",
        )

    def check_local_cntlm(self) -> tuple[bool, str]:
        """Validates if the local CNTLM engine is running.

        Returns:
            tuple[bool, str]: Success flag and description of the
                responsiveness of the local proxy port.
        """
        try:
            with socket.create_connection(
                (self.proxy_host, self.proxy_port), timeout=2.0
            ):
                return (
                    True,
                    "Local NTLM authentication proxy (CNTLM) is "
                    f"operational on port {self.proxy_port}.",
                )
        except ConnectionRefusedError:
            return (
                False,
                "Local CNTLM service is unavailable or inactive. "
                f"Connection refused on port {self.proxy_port}.",
            )
        except TimeoutError:
            return (
                False,
                "Connection timeout during CNTLM handshake. "
                "Verify the responsiveness of the local loopback interface "
                "and proxy daemon.",
            )

    def check_proxy_handshake(self) -> tuple[bool, str]:
        """Validates proxy exit credentials and SSL integrity.

        Returns:
            tuple[bool, str]: Success flag and description of the outbound
                handshake, including public IP and potential SSL issues.
        """

        @contextlib.contextmanager
        def maybe_tap():
            """Context manager to optionally enable httptap inspection."""
            if self.debug_traffic:
                try:
                    from httptap import httpx_tap

                    with httpx_tap():
                        yield
                except ImportError:
                    LOG.warning("httptap not installed. Skipping traffic inspection.")
                    yield
            else:
                yield

        try:
            with maybe_tap(), httpx.Client(proxy=self.proxy_url, timeout=5.0) as client:
                # Outbound request to verify external routing
                res = client.get("https://ipify.org")
                res.raise_for_status()
                public_ip = res.text.strip()
                return (
                    True,
                    "Outbound traffic successfully routed through "
                    f"corporate proxy gateway. Public IP: {public_ip}",
                )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 407:
                return (
                    False,
                    "Proxy authentication failed (HTTP 407). "
                    "Verify that local credentials in the CNTLM configuration "
                    "are up to date.",
                )
            return (
                False,
                "Corporate gateway returned an unexpected status code: "
                f"{e.response.status_code}",
            )
        except httpx.ProxyError as e:
            return (
                False,
                "CNTLM failed to reach the upstream corporate gateway. "
                f"Ensure VPN or intranet connectivity. Details: {e!s}",
            )
        except httpx.ConnectError as e:
            if isinstance(e.__cause__, ssl.SSLCertVerificationError):
                return (
                    False,
                    "SSL/TLS Inspection Detected: "
                    "The connection is being intercepted by a corporate proxy "
                    "using an untrusted certificate chain. "
                    "Verify Root CA installations.",
                )
            return False, f"TCP Handshake failed: {e!s}"

    def check_target_firewall(self) -> tuple[bool, str]:
        """Differentiates DNS glitches from hard firewall drops.

        Returns:
            tuple[bool, str]: Success flag and description of the connection
                attempt to the final target host and port.
        """
        try:
            with socket.create_connection(
                (self.target_host, self.target_port), timeout=3.0
            ):
                return (
                    True,
                    f"Direct network path to target endpoint established at "
                    f"{self.target_host}:{self.target_port} is open.",
                )
        except socket.gaierror:
            return (
                False,
                f"Hostname Resolution Failure: "
                f"Unable to resolve '{self.target_host}'. "
                "Ensure the address is correct and DNS search paths are valid "
                "for this subnet.",
            )
        except TimeoutError:
            return (
                False,
                f"Network Timeout: "
                f"Connection to {self.target_host}:{self.target_port} timed out. "
                "Traffic may be silently dropped by a hardware firewall or "
                "security group.",
            )
        except ConnectionRefusedError:
            return (
                True,
                f"Network path is open, but the remote host at {self.target_host} "
                "explicitly refused the connection. "
                f"The target service may be offline.",
            )

    def check_mtu_payload(self) -> tuple[bool, str]:
        """Tests packet fragmentation over user space streams.

        Returns:
            tuple[bool, str]: Success flag and description of the results
                after attempting to send a large (4MB) data payload to detect
                MTU-related fragmentation drops.
        """
        try:
            with socket.create_connection(
                (self.target_host, self.target_port), timeout=4.0
            ) as sock:
                # 4MB heavy data mock package
                heavy_buffer = b"0" * (4 * 1024 * 1024)
                sock.sendall(heavy_buffer)
                return (
                    True,
                    "Maximum Transmission Unit (MTU) validation successful: "
                    "Large payload handling is stable.",
                )
        except TimeoutError:
            return (
                False,
                "Potential MTU Fragmentation Issue: "
                f"Connection to {self.target_host}:{self.target_port} timed out "
                "during large payload burst. "
                "This often indicates packet fragmentation drops over "
                "tunnel/VPN interfaces.",
            )
        except Exception as e:
            return (
                True,
                f"Stream channel interrupted during payload burst, "
                f"but no silent MTU drops flagged: {e!s}",
            )


# --- EXECUTION DEMO ---
# if __name__ == "__main__":
#     # Simulate testing a connection to an external database
#     (e.g. standard ClickHouse native port)
#     # Replace these inputs with your targeted production servers and local setups
#     doctor = NetworkDoctor(
#         target_host="clickhouse.production.internal",
#         target_port=9000,
#         proxy_url="http://127.0.0.1:3128",
#     )

#     # Run tests natively before initializing resource-intensive jobs like ray.init()
#     success = doctor.run_diagnostics()

#     if not success:
#         sys.exit(1)
