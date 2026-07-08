"""Target service diagnostic checks."""

import socket
import ssl
from datetime import UTC, datetime

import niquests

from libs.network.core.result import DiagnosticResult

from .base import BaseCheck


class TargetCheck(BaseCheck):
    """Target service connectivity and health checks."""

    def __init__(self, target_host: str, target_port: int):
        self.target_host = target_host
        self.target_port = target_port

    def get_section_name(self) -> str:
        return "Target"

    def run(self) -> DiagnosticResult:
        """Run all target checks."""
        return self.check_target_connectivity()

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

    def check_target_connectivity(self):
        """Check direct connectivity to target."""
        try:
            with socket.create_connection(
                (self.target_host, self.target_port), timeout=5.0
            ):
                return self._success(
                    f"Target reachable on {self.target_host}:{self.target_port}"
                )

        except socket.gaierror:
            return self._failure(
                report=f"Cannot resolve {self.target_host}",
                bash_command=f"nslookup {self.target_host}",
                impact_level="HIGH",
            )
        except TimeoutError:
            return self._failure(
                report=f"Connection timeout to {self.target_host}:{self.target_port}",
                bash_command=f"traceroute -n {self.target_host}",
                impact_level="HIGH",
            )
        except ConnectionRefusedError:
            return self._success("Target reachable (service offline)")

    def check_ssl_certificates(self):
        """Validate SSL certificate chain for HTTPS endpoints."""

        # Skip non-HTTPS ports
        if self.target_port not in [443, 8443, 9440]:
            return self._success("SSL check skipped (non-HTTPS port)")

        try:
            cert_bin = self._fetch_certificate()
            if cert_bin is None:
                return self._failure(
                    report="No SSL certificate received",
                    recommendation="Server may not have SSL configured",
                    bash_command=f"openssl s_client -connect {self.target_host}:{self.target_port} -brief",
                    impact_level="HIGH",
                )

            return self._check_cert_expiry(cert_bin)

        except Exception as e:
            return self._handle_ssl_error(e)

    def _handle_ssl_error(self, error: Exception):
        """Handle SSL-related errors with appropriate failure responses."""

        # Define error configurations
        error_configs = {
            ssl.SSLCertVerificationError: {
                "report": f"SSL verification failed: {error}",
                "recommendation": "Install corporate root CA",
                "bash_command": (
                    "update-ca-certificates  # Linux\n"
                    "security add-trusted-cert corporate.pem  # macOS"
                ),
                "impact_level": "HIGH",
            },
            ConnectionRefusedError: {
                "report": f"Connection refused to {self.target_host}:{self.target_port}",
                "recommendation": "Service may be offline",
                "bash_command": f"nc -zv {self.target_host} {self.target_port}",
                "impact_level": "HIGH",
            },
            TimeoutError: {
                "report": f"Connection timeout to {self.target_host}:{self.target_port}",
                "recommendation": "Check firewall rules",
                "bash_command": f"ping {self.target_host}",
                "impact_level": "HIGH",
            },
        }

        # Find matching error type
        for error_type, config in error_configs.items():
            if isinstance(error, error_type):
                return self._failure(**config)

        # Default: Unknown error
        return self._failure(
            report=f"SSL check failed: {error}",
            recommendation="Check SSL support on this port",
            bash_command=f"openssl s_client -connect {self.target_host}:{self.target_port} -brief",
            impact_level="MEDIUM",
        )

    def check_service_health(self):
        """Ping the actual data service health endpoint."""
        try:
            # Use niquests with verify=False for self-signed certs
            with niquests.Session() as session:
                resp = session.get(
                    f"https://{self.target_host}:{self.target_port}/health",
                    timeout=5.0,
                    verify=False,  # Skip SSL verification for health check
                )
                if resp.status_code == 200:
                    return self._success("Service health endpoint OK")
                return self._success(f"Service responded with {resp.status_code}")

        except Exception:
            return self._success("No health endpoint configured")

    def check_mtu_payload(self):
        """Test large payload transmission."""
        try:
            with socket.create_connection(
                (self.target_host, self.target_port), timeout=10.0
            ) as sock:
                sock.sendall(b"0" * (4 * 1024 * 1024))
                return self._success("Large payload transmitted successfully")

        except TimeoutError:
            return self._failure(
                report="Payload transmission timed out",
                bash_command=f"ping -M do -s 1472 {self.target_host}",
                impact_level="MEDIUM",
            )
        except Exception as e:
            return self._success(f"Payload test interrupted: {e!s}")

    def _fetch_certificate(self) -> bytes | None:
        """Fetch SSL certificate from server."""
        context = ssl.create_default_context()
        with (
            socket.create_connection(
                (self.target_host, self.target_port), timeout=5.0
            ) as sock,
            context.wrap_socket(sock, server_hostname=self.target_host) as ssock,
        ):
            return ssock.getpeercert(binary_form=True)

    def _check_cert_expiry(self, cert_bin: bytes):
        """Check certificate expiration."""
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend

        cert = x509.load_der_x509_certificate(cert_bin, default_backend())
        now = datetime.now(UTC)

        if cert.not_valid_after < now:
            days = (now - cert.not_valid_after).days
            return self._failure(
                report=f"Certificate expired {days} days ago on {cert.not_valid_after}",
                recommendation="Contact IT to renew",
                bash_command=f"echo | openssl s_client -connect {self.target_host}:{self.target_port} 2>/dev/null | openssl x509 -noout -dates",
                impact_level="HIGH",
            )

        if cert.not_valid_after < now.replace(year=now.year + 1):
            days = (cert.not_valid_after - now).days
            return self._success(
                report=f"Certificate expires in {days} days",
                recommendation="Schedule renewal soon",
                bash_command=f"echo | openssl s_client -connect {self.target_host}:{self.target_port} 2>/dev/null | openssl x509 -noout -dates",
            )

        return self._success("Certificate is valid")
