"""
Email notification module for Edison SAF Import.

Handles sending email reports with import summaries and logs.
"""

import datetime
import logging
import os
import platform
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional


class EmailNotifier:
    """Handles email notifications for import operations."""

    def __init__(
        self,
        smtp_server: str,
        smtp_port: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize email notifier with SMTP configuration.

        Args:
            smtp_server: SMTP server hostname
            smtp_port: SMTP server port
            username: SMTP username (optional)
            password: SMTP password (optional)
            logger: Logger instance (optional)
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.logger = logger or logging.getLogger(__name__)

    def send_import_report(
        self,
        log_filename: str,
        total_imports: int,
        successful_imports: int,
        error_count: int,
        collections: List[str],
        container_name: str,
        source_path: str,
        error_messages: Optional[List[str]] = None,
        recipients: Optional[List[str]] = None,
        sender: Optional[str] = None,
    ) -> bool:
        """Send email report with import summary.

        Args:
            log_filename: Path to log file
            total_imports: Total number of imports attempted
            successful_imports: Number of successful imports
            error_count: Number of errors encountered
            collections: List of collection names processed
            container_name: Docker container name
            source_path: Source export path
            error_messages: List of error messages (optional)
            recipients: Email recipients (optional)
            sender: Email sender (optional)

        Returns:
            True if email sent successfully, False otherwise
        """
        if not recipients:
            self.logger.error("No email recipients provided")
            return False

        try:
            msg = self._create_email_message(
                log_filename=log_filename,
                total_imports=total_imports,
                successful_imports=successful_imports,
                error_count=error_count,
                collections=collections,
                container_name=container_name,
                source_path=source_path,
                error_messages=error_messages,
                recipients=recipients,
                sender=sender,
            )

            self._send_email(msg, sender, recipients)
            self.logger.info(
                f"Email report sent successfully to: {', '.join(recipients)}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
            return False

    def _create_email_message(
        self,
        log_filename: str,
        total_imports: int,
        successful_imports: int,
        error_count: int,
        collections: List[str],
        container_name: str,
        source_path: str,
        error_messages: Optional[List[str]],
        recipients: List[str],
        sender: Optional[str],
    ) -> MIMEMultipart:
        """Create email message with import summary."""
        msg = MIMEMultipart()
        msg['From'] = sender or f"Edison SAF Import <{recipients[0]}>"
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = f"Edison SAF Import Report - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        body = self._create_email_body(
            log_filename=log_filename,
            total_imports=total_imports,
            successful_imports=successful_imports,
            error_count=error_count,
            collections=collections,
            container_name=container_name,
            source_path=source_path,
            error_messages=error_messages,
        )

        msg.attach(MIMEText(body, 'plain'))
        return msg

    def _create_email_body(
        self,
        log_filename: str,
        total_imports: int,
        successful_imports: int,
        error_count: int,
        collections: List[str],
        container_name: str,
        source_path: str,
        error_messages: Optional[List[str]],
    ) -> str:
        """Create email body content."""
        status = self._determine_status(error_count, successful_imports)
        server_name = self._get_server_name()

        body = f"""Edison SAF Import Report
{'='*50}

Import Status: {status}
Completed: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Summary:
- Total imports attempted: {total_imports}
- Successful imports: {successful_imports}
- Failed imports: {total_imports - successful_imports}
- Errors encountered: {error_count}

Container: {container_name}
Source: {source_path}
Target collections: {collections}
"""

        if error_count > 0 and error_messages:
            body += f"\n{'='*50}\nERROR DETAILS:\n{'='*50}\n"
            for i, error in enumerate(error_messages, 1):
                body += f"{i}. {error}\n"

        body += f"\n{'='*50}\nDETAILED LOGS LOCATION:\n{'='*50}\n"
        body += f"Full log file: {log_filename}\n"
        body += f"Server: {server_name}\n"
        body += "\n---\nThis is an automated report from Edison SAF Import script."

        return body

    def _determine_status(self, error_count: int, successful_imports: int) -> str:
        """Determine overall import status."""
        if error_count == 0:
            return "SUCCESS"
        elif successful_imports > 0:
            return "COMPLETED WITH ERRORS"
        else:
            return "FAILED"

    def _get_server_name(self) -> str:
        """Get current server name."""
        try:
            if hasattr(os, 'uname'):
                return os.uname().nodename
            else:
                return platform.node()
        except Exception:
            return "current server"

    def _send_email(
        self,
        msg: MIMEMultipart,
        sender: Optional[str],
        recipients: List[str]
    ) -> None:
        """Send email via SMTP."""
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            if self.username and self.password:
                if self.smtp_port in (587, 465):
                    server.starttls()
                server.login(self.username, self.password)

            server.sendmail(sender or recipients[0], recipients, msg.as_string())
