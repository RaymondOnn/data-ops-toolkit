"""Proxy and authentication diagnostic checks."""

import os
import socket
from contextlib import contextmanager

import niquests
from niquests.exceptions import HTTPError

from libs.network.core.result import DiagnosticResult

from .base import BaseCheck


class ProxyCheck(BaseCheck):
    """Proxy configuration and authentication checks."""

    def __init__(self, proxy_url: str, debug_traffic: bool = False):
        self.proxy_url = proxy_url
        self.debug_traffic = debug_traffic
        self.proxy_host, self.proxy_port = self._parse_proxy_url(proxy_url)

    def get_section_name(self) -> str:
        return "Proxy"

    def run(self) -> DiagnosticResult:
        """Run all proxy checks."""
        return self.check_environment_variables()

    def check_environment_variables(self):
        """Validate proxy environment variables."""
        mismatches = []
        for key in ["http_proxy", "https_proxy", "no_proxy"]:
            if bool(os.environ.get(key)) != bool(os.environ.get(key.upper())):
                mismatches.append(key)

        if mismatches:
            return self._failure(
                report=f"Inconsistent casing: {', '.join(mismatches)}",
                recommendation="Standardize proxy variables",
                bash_command="export http_proxy=http://127.0.0.1:3128 && export https_proxy=http://127.0.0.1:3128",
                auto_fix=True,
                fix_command="export http_proxy=http://127.0.0.1:3128 && export https_proxy=http://127.0.0.1:3128",
                impact_level="HIGH",
            )

        return self._success("Environment variables consistent")

    def check_local_proxy(self):
        """Validate if the local proxy engine is running."""
        try:
            with socket.create_connection(
                (self.proxy_host, self.proxy_port), timeout=2.0
            ):
                return self._success(
                    f"Proxy responsive on {self.proxy_host}:{self.proxy_port}"
                )

        except ConnectionRefusedError:
            return self._failure(
                report=f"Connection refused on port {self.proxy_port}",
                recommendation="Start proxy daemon",
                bash_command="ps aux | grep cntlm",
                auto_fix=True,
                fix_command="cntlm -c /etc/cntlm.conf &",
                impact_level="HIGH",
            )
        except TimeoutError:
            return self._failure(
                report=f"Connection timeout on port {self.proxy_port}",
                bash_command=f"telnet {self.proxy_host} {self.proxy_port}",
                impact_level="HIGH",
            )

    def check_ntlm_auth(self):
        """Verify NTLM authentication path through proxy."""
        try:
            # Use niquests which supports NTLM auth natively
            with niquests.Session() as session:
                response = session.get(
                    f"http://{self.proxy_host}:{self.proxy_port}",
                    timeout=5.0,
                    proxies={"http": self.proxy_url, "https": self.proxy_url},
                )

                if response.status_code == 407:
                    return self._failure(
                        report="NTLM authentication required",
                        recommendation="Check proxy credentials",
                        bash_command="cat /etc/cntlm.conf | grep -E '(Username|Domain)'",
                        impact_level="HIGH",
                    )
                return self._success("Proxy is responsive")

        except ImportError:
            return self._success("Check skipped (niquests not installed)")
        except Exception as e:
            return self._failure(
                report=f"NTLM check failed: {e!s}",
                bash_command="curl -x http://127.0.0.1:3128 https://ipify.org -v",
                impact_level="MEDIUM",
            )

    def check_proxy_handshake(self):
        """Validate proxy exit credentials and SSL integrity."""
        try:
            with self._maybe_tap(), niquests.Session() as session:
                response = session.get(
                    "https://ipify.org",
                    proxies={"https": self.proxy_url},
                    timeout=10.0,
                )
                response.raise_for_status()

        except HTTPError as e:
            if not e.response:
                return self._failure(
                    report="Proxy handshake failed (no response)",
                    recommendation="Check proxy configuration",
                    bash_command=f"curl -x {self.proxy_url} https://ipify.org -v",
                    impact_level="HIGH",
                )
            if e.response.status_code == 407:
                return self._failure(
                    report="Proxy authentication failed (HTTP 407)",
                    recommendation="Verify proxy credentials",
                    bash_command="cntlm -c /etc/cntlm.conf -f",
                    impact_level="HIGH",
                )
            return self._failure(
                report=f"HTTP {e.response.status_code}",
                bash_command="tail -f /var/log/cntlm.log",
                impact_level="HIGH",
            )
        except Exception as e:
            return self._failure(
                report=f"Proxy handshake failed: {e!s}",
                bash_command=f"telnet {self.proxy_host} {self.proxy_port}",
                impact_level="HIGH",
            )
        else:
            if response:
                public_ip = response.text.strip()
                return self._success(f"Proxy working. Public IP: {public_ip}")

    def _parse_proxy_url(self, proxy_url: str) -> tuple[str, int]:
        """Parse proxy URL into host and port."""
        from urllib.parse import urlparse

        try:
            parsed = urlparse(proxy_url)
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port or 3128
            return host, port
        except Exception:
            return "127.0.0.1", 3128

    @contextmanager
    def _maybe_tap(self):
        """Context manager to optionally enable traffic inspection."""
        if self.debug_traffic:
            try:
                # niquests supports the same httptap integration
                from httptap import HTTPTapAnalyzer  # httptap works with niquests too

                with HTTPTapAnalyzer():
                    yield
            except ImportError:
                yield
        else:
            yield
