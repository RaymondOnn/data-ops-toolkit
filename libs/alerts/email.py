import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

LOG = logger


class EmailClient:
    """Robust SMTP Client for sending transaction and alert emails."""

    def __init__(
        self,
        smtp_host: str | None = None,
        smtp_port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = True,
        default_sender: str | None = None,
        timeout: float = 10.0,
    ):
        self.host = smtp_host or os.getenv("SMTP_HOST") or "localhost"
        self.port = smtp_port or int(os.getenv("SMTP_PORT", "587"))
        self.username = username or os.getenv("SMTP_USER")
        self.password = password or os.getenv("SMTP_PASSWORD")
        self.use_tls = use_tls
        self.default_sender = default_sender or os.getenv(
            "SMTP_SENDER", self.username or "alerts@system.local"
        )
        self.timeout = timeout

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((smtplib.SMTPException, TimeoutError, OSError)),
        reraise=True,
    )
    def send(
        self,
        recipients: str | list[str],
        subject: str,
        body_text: str,
        body_html: str | None = None,
        sender_email: str | None = None,
    ) -> bool:
        """Sends an email with exponential backoff retries.

        Args:
            recipients: Single email string or list of recipient email addresses.
            subject: Email subject line.
            body_text: Fallback plain-text content.
            body_html: Rich HTML formatted content (optional).
            sender_email: Sender email address (overrides default_sender if provided).

        Returns:
            bool: True if sent successfully.
        """
        to_addresses = [recipients] if isinstance(recipients, str) else recipients

        if not to_addresses:
            raise ValueError("Recipient list cannot be empty.")

        # Use explicitly passed sender, or fall back to class default
        from_address = sender_email or self.default_sender
        if not from_address:
            raise ValueError("Sender email address cannot be empty or None.")

        msg = MIMEMultipart("alternative")
        msg["From"] = from_address
        msg["To"] = ", ".join(to_addresses)
        msg["Subject"] = subject

        # Attach plain-text first, then HTML as preferred alternative
        msg.attach(MIMEText(body_text, "plain", "utf-8"))
        if body_html:
            msg.attach(MIMEText(body_html, "html", "utf-8"))

        LOG.debug(f"Connecting to SMTP server {self.host}:{self.port}...")

        try:
            with smtplib.SMTP(self.host, self.port, timeout=self.timeout) as server:
                server.ehlo()
                if self.use_tls:
                    server.starttls()
                    server.ehlo()

                if self.username and self.password:
                    server.login(self.username, self.password)

                server.sendmail(from_address, to_addresses, msg.as_string())
                LOG.info(
                    f"Alert email sent successfully from '{from_address}' to {len(to_addresses)} recipient(s)."
                )
                return True

        except Exception as e:
            LOG.error(f"Failed to send email via {self.host}:{self.port}: {e}")
            raise
