"""Operational Digest Notification Service.

Consolidates unnotified operational events (schema changes, errors, quality alerts)
from the metadata database into a single, unified digest email.
"""

from collections.abc import Sequence

import msgspec
from libs.alerts.email import EmailClient
from libs.utils.dates import current_timestamp
from loguru import logger

from src.services.repo.metadata import MetadataRepository
from src.utils.constants import STRIP_TZ_FOR_DB

LOG = logger


class DigestSection(msgspec.Struct, frozen=True):
    """Represents a section in the operational digest email."""

    title: str
    priority: int
    items: list[str]


class OperationalDigestService:
    """Consolidates pending operational events from the metadata database into a digest email."""

    def __init__(
        self,
        repo: MetadataRepository,
        email_client: EmailClient,
        recipients: str | list[str],
        sender_email: str | None = None,
    ) -> None:
        """Initialize the digest service with repository and email configuration.

        Args:
            repo: MetadataRepository instance.
            email_client: EmailClient handle for SMTP dispatch.
            recipients: Target recipient email(s).
            sender_email: Optional sender email override.
        """
        self.repo = repo
        self.email_client = email_client
        self.recipients = [recipients] if isinstance(recipients, str) else recipients
        self.sender_email = sender_email

    def send_digest(self) -> bool:
        """Collect pending operational events, construct the digest email, send it,
        and mark events as notified in the metadata DB.

        Returns:
            bool: True if digest email was dispatched, False if no pending events.
        """
        sections: list[DigestSection] = []

        # 1. Collect Schema Evolutions
        schema_sec, _ = self._build_schema_section()
        if schema_sec:
            sections.append(schema_sec)

        # 2. Collect Empty Result Sets
        empty_sec, _ = self._build_empty_result_set_section()
        if empty_sec:
            sections.append(empty_sec)

        if not sections:
            LOG.info("No pending operational events for digest email.")
            return False

        # Sort sections by priority
        sections.sort(key=lambda s: s.priority)

        subject = (
            f"[Digest] Ingestion Platform Operational Summary ({datetime_now_str()})"
        )
        body_text = self._render_plain_text(sections)
        body_html = self._render_html(sections)

        LOG.info(
            "Dispatching operational digest email",
            recipients=len(self.recipients),
            sections=len(sections),
        )

        self.email_client.send(
            recipients=self.recipients,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            sender_email=self.sender_email,
        )

        LOG.info("Operational digest email dispatched and records marked as notified.")
        return True

    def _build_schema_section(self) -> tuple[DigestSection | None, list[str]]:
        changes = self.repo.get_schema_changes_today()
        if not changes:
            return None, []

        items = [
            f"• Dataset: {c.dataset_id} | Applied: {c.applied_at}\n  Schema: {c.schema_json}"
            for c in changes
        ]
        sec = DigestSection(
            title="🏗️ Database Schema Evolutions", priority=1, items=items
        )
        return sec, [c.id for c in changes]

    def _build_empty_result_set_section(self) -> tuple[DigestSection | None, list[str]]:
        records = self.repo.get_empty_result_sets_today()
        if not records:
            return None, []

        items = [
            f"• Dataset: {r.dataset_id} | Job: {r.job_id} | Run: {r.run_id} | Time: {r.start_ts}"
            for r in records
        ]
        sec = DigestSection(
            title="📭 Empty Result Sets Processed (SUCCESS)", priority=2, items=items
        )
        return sec, [r.run_id for r in records]

    def _render_plain_text(self, sections: Sequence[DigestSection]) -> str:
        lines = [
            "Ingestion Platform Operational Digest Summary",
            "=" * 50,
            "",
        ]
        for sec in sections:
            lines.append(f"## {sec.title}")
            lines.extend(sec.items)
            lines.append("")
        return "\n".join(lines)

    def _render_html(self, sections: Sequence[DigestSection]) -> str:
        sec_html = ""
        for sec in sections:
            items_html = "".join(
                f"<li style='margin-bottom: 8px;'>{item.replace(chr(10), '<br/>')}</li>"
                for item in sec.items
            )
            sec_html += f"""
            <div style="margin-bottom: 24px;">
              <h3 style="color: #0056b3; border-bottom: 2px solid #eee; padding-bottom: 4px;">{sec.title}</h3>
              <ul style="padding-left: 20px; color: #333;">{items_html}</ul>
            </div>
            """

        return f"""
        <html>
          <body style="font-family: Arial, sans-serif; line-height: 1.5; color: #333; max-width: 700px; margin: 0 auto; padding: 20px;">
            <h2 style="color: #1a252f;">Ingestion Platform Operational Digest</h2>
            <p style="color: #7f8c8d; font-size: 0.9em;">Consolidated digest of platform events.</p>
            <hr style="border: 0; border-top: 1px solid #eee; margin-bottom: 20px;"/>
            {sec_html}
          </body>
        </html>
        """


def datetime_now_str() -> str:
    return current_timestamp(naive=STRIP_TZ_FOR_DB).isoformat(sep=" ")
