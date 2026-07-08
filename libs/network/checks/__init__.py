"""Diagnostic checks package."""

from .base import BaseCheck
from .network import NetworkCheck
from .proxy import ProxyCheck
from .system import SystemCheck
from .target import TargetCheck


def create_all_checks(
    target_host: str,
    target_port: int,
    proxy_url: str,
    vpn_prefixes: list[str],
    debug_traffic: bool = False,
) -> list:
    """Create all diagnostic checks."""
    from libs.network.core.pipeline import CheckDefinition

    # Initialize checks with shared context
    checks = []

    # System checks
    system = SystemCheck()
    checks.extend(
        [
            CheckDefinition(
                section="Socket/File Descriptor Limits",
                method=system.check_socket_limits,
                required=True,
            ),
            CheckDefinition(
                section="Packet Capture Tools",
                method=system.check_packet_capture,
                required=False,
            ),
        ]
    )

    # Network checks
    network = NetworkCheck(target_host, target_port, vpn_prefixes)
    checks.extend(
        [
            CheckDefinition(
                section="Corporate VPN Connectivity",
                method=network.check_vpn_presence,
                required=True,
            ),
            CheckDefinition(
                section="DNS Resolution",
                method=network.check_dns_resolution,
                required=True,
            ),
            CheckDefinition(
                section="Path MTU Discovery",
                method=network.check_path_mtu,
                required=False,
            ),
            CheckDefinition(
                section="Bandwidth & Latency",
                method=network.check_bandwidth_latency,
                required=False,
            ),
        ]
    )

    # Proxy checks
    proxy = ProxyCheck(proxy_url, debug_traffic)
    checks.extend(
        [
            CheckDefinition(
                section="Environment Variables",
                method=proxy.check_environment_variables,
                required=True,
            ),
            CheckDefinition(
                section="Local CNTLM Daemon",
                method=proxy.check_local_proxy,
                dependencies=["Environment Variables"],
                required=True,
            ),
            CheckDefinition(
                section="NTLM Authentication Path",
                method=proxy.check_ntlm_auth,
                dependencies=["Local CNTLM Daemon"],
                required=True,
            ),
            CheckDefinition(
                section="Proxy Handshake",
                method=proxy.check_proxy_handshake,
                dependencies=["NTLM Authentication Path"],
                required=True,
            ),
        ]
    )

    # Target checks
    target = TargetCheck(target_host, target_port)
    checks.extend(
        [
            CheckDefinition(
                section="DNS Resolution",
                method=target.check_dns_resolution,
                required=True,
            ),
            CheckDefinition(
                section="Target Connectivity",
                method=target.check_target_connectivity,
                dependencies=["DNS Resolution", "Corporate VPN Connectivity"],
                required=True,
            ),
            CheckDefinition(
                section="SSL/TLS Certificates",
                method=target.check_ssl_certificates,
                dependencies=["Target Connectivity"],
                required=False,
            ),
            CheckDefinition(
                section="Target Service Health",
                method=target.check_service_health,
                dependencies=["Target Connectivity"],
                required=False,
            ),
            CheckDefinition(
                section="Large Payload (MTU)",
                method=target.check_mtu_payload,
                dependencies=["Target Connectivity"],
                required=False,
            ),
        ]
    )

    return checks


__all__ = [
    "BaseCheck",
    "NetworkCheck",
    "ProxyCheck",
    "SystemCheck",
    "TargetCheck",
    "create_all_checks",
]
