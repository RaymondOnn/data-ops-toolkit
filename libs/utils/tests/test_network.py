import socket
from unittest.mock import MagicMock, patch

import httpx
import pytest
from libs.utils.network import NetworkDoctor


class TestNetworkDoctor:
    """Unit tests for the NetworkDoctor diagnostic suite."""

    @pytest.fixture
    def doctor(self):
        """Returns a NetworkDoctor instance for testing."""
        return NetworkDoctor(
            target_host="test.host", target_port=80, proxy_url="http://127.0.0.1:3128"
        )

    @patch("psutil.net_if_addrs")
    def test_check_vpn_presence_success(self, mock_if_addrs, doctor):
        """GIVEN a machine with a corporate IP address (10.x.x.x)
        THEN return success as a VPN connection is detected
        WHEN check_vpn_presence is called
        """
        mock_addr = MagicMock()
        mock_addr.family = socket.AF_INET
        mock_addr.address = "10.1.2.3"
        mock_if_addrs.return_value = {"eth0": [mock_addr]}

        passed, report = doctor.check_vpn_presence()
        assert passed is True
        assert "VPN connectivity verified" in report

    @patch("psutil.net_if_addrs")
    def test_check_vpn_presence_fail(self, mock_if_addrs, doctor):
        """GIVEN a machine with only public or home IPs
        THEN return failure as no corporate VPN prefix is found
        WHEN check_vpn_presence is called
        """
        mock_addr = MagicMock()
        mock_addr.family = socket.AF_INET
        mock_addr.address = "8.8.8.8"
        mock_if_addrs.return_value = {"eth0": [mock_addr]}

        passed, _ = doctor.check_vpn_presence()
        assert passed is False

    @patch("socket.create_connection")
    def test_check_local_cntlm_success(self, mock_conn, doctor):
        """GIVEN a responsive local port 3128
        THEN return success indicating CNTLM is running
        WHEN check_local_cntlm is called
        """
        passed, report = doctor.check_local_cntlm()
        assert passed is True
        assert "operational on port 3128" in report

    @patch("socket.create_connection", side_effect=ConnectionRefusedError)
    def test_check_local_cntlm_refused(self, mock_conn, doctor):
        """GIVEN a closed local port 3128
        THEN return failure as the proxy service is likely down
        WHEN check_local_cntlm is called
        """
        passed, report = doctor.check_local_cntlm()
        assert passed is False
        assert "Connection refused" in report

    @patch("httpx.Client.get")
    def test_check_proxy_handshake_success(self, mock_get, doctor):
        """GIVEN a functional proxy gateway
        THEN return success with the reported public IP
        WHEN check_proxy_handshake is called
        """
        mock_response = MagicMock()
        mock_response.text = "1.2.3.4"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        passed, report = doctor.check_proxy_handshake()
        assert passed is True
        assert "Public IP: 1.2.3.4" in report

    @patch(
        "httpx.Client.get",
        side_effect=httpx.HTTPStatusError(
            "407", request=MagicMock(), response=MagicMock(status_code=407)
        ),
    )
    def test_check_proxy_handshake_auth_fail(self, mock_get, doctor):
        """GIVEN proxy credentials are stale (HTTP 407)
        THEN return failure indicating authentication issues
        WHEN check_proxy_handshake is called
        """
        passed, report = doctor.check_proxy_handshake()
        assert passed is False
        assert "Proxy authentication failed" in report

    @patch("socket.create_connection")
    def test_check_target_firewall_success(self, mock_conn, doctor):
        """GIVEN a target host that accepts TCP connections
        THEN return success indicating the network path is open
        WHEN check_target_firewall is called
        """
        passed, report = doctor.check_target_firewall()
        assert passed is True
        assert "Direct network path to target endpoint established" in report

    def test_get_report_structure(self, doctor):
        """GIVEN a fresh NetworkDoctor instance
        THEN return a report dictionary with correctly initialized metadata
        WHEN get_report is called
        """
        report = doctor.get_report()
        assert "metadata" in report
        assert report["metadata"]["target_host"] == "test.host"
        assert report["all_passed"] is False  # Results list is empty
