"""Network infrastructure diagnostic checks."""

import contextlib
import socket
import time

import psutil

from libs.network.core.result import DiagnosticResult

from .base import BaseCheck


class NetworkCheck(BaseCheck):
    """Network-level checks for VPN, DNS, MTU, etc."""

    def __init__(self, target_host: str, target_port: int, vpn_prefixes: list[str]):
        self.target_host = target_host
        self.target_port = target_port
        self.vpn_prefixes = vpn_prefixes

    def get_section_name(self) -> str:
        return "Network"

    def run(self) -> DiagnosticResult:
        """Run all network checks."""
        # For now, run VPN presence as the main check
        return self.check_vpn_presence()

    def check_vpn_presence(self):
        """Verify if machine has an IP within corporate ranges."""
        external_ips = self._get_external_ips()

        if not external_ips:
            return self._failure(
                report="No active network interfaces detected",
                bash_command="ip addr show",
                impact_level="HIGH",
            )

        for ip in external_ips:
            if self._is_corporate_ip(ip):
                return self._success(f"VPN detected: IP {ip} in corporate range")

        return self._failure(
            report=f"No corporate VPN IP found. Active IPs: {external_ips[:3]}",
            recommendation="Connect to VPN",
            bash_command="sudo systemctl start openvpn@client",
            impact_level="HIGH",
        )

    def check_dns_resolution(self):
        """Check DNS resolution for the target host."""
        try:
            ips = socket.gethostbyname_ex(self.target_host)[2]
            return self._success(f"Resolved {self.target_host} -> {', '.join(ips[:3])}")
        except socket.gaierror as e:
            return self._failure(
                report=f"DNS resolution failed: {e!s}",
                recommendation="Check DNS configuration",
                bash_command=f"nslookup {self.target_host}",
                impact_level="HIGH",
            )

    def check_path_mtu(self):
        """Discover optimal MTU size to target."""
        try:
            optimal_mtu = self._calculate_optimal_mtu()

            if optimal_mtu < 1400:
                return self._failure(
                    report=f"Low MTU detected: {optimal_mtu}",
                    recommendation="Adjust VPN MTU settings",
                    bash_command=f"ping -M do -s {optimal_mtu} {self.target_host}",
                    impact_level="MEDIUM",
                )
            return self._success(f"Good MTU: {optimal_mtu}")

        except Exception as e:
            return self._failure(
                report=f"MTU detection failed: {e!s}",
                bash_command="ip link show | grep mtu",
                impact_level="LOW",
            )

    def check_bandwidth_latency(self):
        """Measure throughput and latency to target endpoint."""
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
                    return self._failure(
                        report=f"High latency: {avg_rtt:.1f}ms",
                        recommendation="Check VPN route",
                        bash_command=f"ping -c 10 {self.target_host}",
                        impact_level="MEDIUM",
                    )
                return self._success(f"Good latency: {avg_rtt:.1f}ms")

        except Exception as e:
            return self._failure(
                report=f"Latency test failed: {e!s}",
                bash_command=f"ping -c 3 {self.target_host}",
                impact_level="LOW",
            )

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
