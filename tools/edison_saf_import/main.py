#!/usr/bin/env python3
"""
Edison SAF Import to DSpace - Main Entry Point

This script orchestrates the import of Edison exports into DSpace via Docker.
"""

import argparse
import datetime
import logging
from typing import List, Tuple

from . import config
from .importer_logic import EdisonImporter
from .notifier import EmailNotifier


def setup_logging(verbose: bool = False) -> Tuple[logging.Logger, str]:
    """Setup logging configuration.

    Args:
        verbose: Whether to show detailed log messages on screen

    Returns:
        Tuple of (logger, log_filename)
    """
    log_filename = f'/tmp/edison_import_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    # Create formatters
    detailed_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    summary_formatter = logging.Formatter('%(message)s')

    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler - always gets everything with detailed format
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)

    # Console handler - summary format for summary messages, detailed for verbose
    console_handler = logging.StreamHandler()
    if verbose:
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(detailed_formatter)
    else:
        # Only show summary messages (we'll use a custom filter)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(summary_formatter)
        console_handler.addFilter(lambda record: getattr(record, 'summary', False))

    logger.addHandler(console_handler)

    return logger, log_filename


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(description='Edison SAF Import to DSpace')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Show log messages on screen (default: log to file only)')
    parser.add_argument('--email', action='store_true',
                        help='Send email report with summary and log file')
    parser.add_argument('--email-to', nargs='+',
                        help='Email recipients (default: configured recipients)')
    parser.add_argument('--email-from',
                        help='Sender email address (default: configured sender)')
    parser.add_argument('--smtp-server', default=config.SMTP_SERVER,
                        help=f'SMTP server (default: {config.SMTP_SERVER})')
    parser.add_argument('--smtp-port', type=int, default=config.SMTP_PORT,
                        help=f'SMTP port (default: {config.SMTP_PORT})')
    parser.add_argument('--smtp-username',
                        help='SMTP username (optional, for authenticated SMTP)')
    parser.add_argument('--smtp-password',
                        help='SMTP password (optional, for authenticated SMTP)')
    return parser


def log_summary(
    logger: logging.Logger,
    total_imports: int,
    successful_imports: int,
    error_count: int
) -> None:
    """Log import summary to both file and console."""
    # Create summary log entries that go to both file and console
    logger.info("=" * 50, extra={'summary': True})
    logger.info("FINAL SUMMARY", extra={'summary': True})
    logger.info("=" * 50, extra={'summary': True})
    logger.info(f"Total imports attempted: {total_imports}", extra={'summary': True})
    logger.info(f"Successful imports: {successful_imports}", extra={'summary': True})
    logger.info(
        f"Failed imports: {total_imports - successful_imports}", extra={'summary': True})

    if error_count > 0:
        logger.info(f"\nErrors encountered: {error_count} (see log file for details)", extra={
                    'summary': True})

    if successful_imports == total_imports:
        logger.info("\n✓ All imports completed successfully!", extra={'summary': True})
    elif successful_imports > 0:
        logger.info("\n⚠ Some imports completed successfully, but there were failures", extra={
                    'summary': True})
    else:
        logger.info("\n✗ All imports failed", extra={'summary': True})


def send_email_report(
    args: argparse.Namespace,
    logger: logging.Logger,
    log_filename: str,
    total_imports: int,
    successful_imports: int,
    error_messages: List[str],
) -> bool:
    """Send email report if requested."""
    if not args.email:
        return True

    logger.info("\nSending email report...", extra={'summary': True})

    notifier = EmailNotifier(
        smtp_server=args.smtp_server,
        smtp_port=args.smtp_port,
        username=args.smtp_username,
        password=args.smtp_password,
        logger=logger,
    )

    recipients = args.email_to or config.DEFAULT_RECIPIENTS
    sender = args.email_from or config.DEFAULT_SENDER

    success = notifier.send_import_report(
        log_filename=log_filename,
        total_imports=total_imports,
        successful_imports=successful_imports,
        error_count=len(error_messages),
        collections=list(config.COLLECTIONS.keys()),
        container_name=config.CONTAINER_NAME,
        source_path=config.BASE_EXPORT_PATH,
        error_messages=error_messages,
        recipients=recipients,
        sender=sender,
    )

    if success:
        logger.info(f"✓ Email report sent to: {', '.join(recipients)}", extra={
                    'summary': True})
    else:
        logger.info("✗ Failed to send email report (check log for details)",
                    extra={'summary': True})

    return success


def main() -> int:
    """Main execution function."""
    parser = create_argument_parser()
    args = parser.parse_args()

    logger, log_filename = setup_logging(args.verbose)

    logger.info(f"Edison SAF Import starting... Logs saved to: {log_filename}", extra={
                'summary': True})
    if not args.verbose:
        logger.info("Use -v or --verbose to see detailed output on screen.\n",
                    extra={'summary': True})

    importer = None
    total_imports = 0
    successful_imports = 0
    email_sent = True
    try:
        importer = EdisonImporter(verbose=args.verbose, logger=logger)
        # Copy data to container
        logger.info("Step 1: Copying data to container...", extra={'summary': True})

        if not importer.copy_data_to_container():
            logger.error("[ERROR] Failed to copy data. Exiting.")
            logger.error("ERROR: Failed to copy data. Check log file for details.", extra={
                         'summary': True})
            return 1

        # Process imports
        logger.info("Step 2: Finding and processing exports...", extra={'summary': True})

        total_imports, successful_imports = importer.process_imports()

        if total_imports == 0:
            logger.error("[ERROR] No imports processed. Exiting.")
            logger.error("ERROR: No imports processed. Check log file for details.", extra={
                         'summary': True})
            return 1

        # Log final summary
        logger.info(f"\n{'='*60}")
        logger.info("[SUMMARY] FINAL SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total imports attempted: {total_imports}")
        logger.info(f"Successful imports: {successful_imports}")
        logger.info(f"Failed imports: {total_imports - successful_imports}")

        if importer.error_messages:
            logger.error(f"Error messages ({len(importer.error_messages)} errors):")
            for i, error in enumerate(importer.error_messages, 1):
                logger.error(f"  {i}. {error}")

        # Log summary to both file and console
        log_summary(logger, total_imports, successful_imports,
                    len(importer.error_messages))

        # Log completion status
        if successful_imports == total_imports:
            logger.info("[SUCCESS] All imports completed successfully!")
        elif successful_imports > 0:
            logger.warning(
                "[WARNING] Some imports completed successfully, but there were failures")
        else:
            logger.error("[ERROR] All imports failed")

    except Exception as exc:
        logger.error(f"[FATAL] Unhandled exception: {exc}", exc_info=True)
    finally:
        # Always cleanup
        if importer is not None:
            logger.info("\nCleaning up...", extra={'summary': True})
            importer.cleanup_container()

    logger.info("\n[FINISHED] Edison SAF Import completed.")

    # Send email report
    if importer is not None:
        email_sent = send_email_report(
            args, logger, log_filename, total_imports, successful_imports, importer.error_messages
        )

        email_msg = " Email report sent." if args.email and email_sent else ""
        logger.info(f"\nProcess completed.{email_msg} Full logs available at: {log_filename}", extra={
                    'summary': True})

    # Return non-zero exit code if all imports failed
    if total_imports > 0 and successful_imports == 0:
        return 1

    # Return non-zero exit code if email was requested but failed to send
    if args.email and not email_sent:
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
