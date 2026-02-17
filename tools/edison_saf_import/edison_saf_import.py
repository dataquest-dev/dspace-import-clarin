#!/usr/bin/env python3
"""
Edison SAF Import to DSpace - POC Script

This script copies Edison exports from /opt/edison_exports into a Docker container,
runs DSpace SAF import for each collection, and cleans up afterwards.
"""

import subprocess
import os
import datetime
import logging
import argparse
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def setup_logging(verbose=False):
    """Setup logging configuration based on verbosity"""
    log_filename = f'/tmp/edison_import_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    # Configure logging handlers
    handlers = [logging.FileHandler(log_filename)]

    if verbose:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers,
        force=True  # Reset any existing configuration
    )

    return logging.getLogger(__name__), log_filename


def show_progress(current, total, collection_name=""):
    """Display progress bar"""
    if total == 0:
        percentage = 0
    else:
        percentage = (current / total) * 100

    bar_length = 40
    filled_length = int(bar_length * current // total) if total > 0 else 0
    bar = '█' * filled_length + '░' * (bar_length - filled_length)

    status = f"Processing: {collection_name}" if collection_name else "Processing..."

    # Use \r to overwrite the same line
    sys.stdout.write(f"\r[{bar}] {percentage:.1f}% ({current}/{total}) - {status}")
    sys.stdout.flush()


def send_email(log_filename, total_imports, successful_imports, error_count, recipients=None, sender=None):
    """Send email with import summary and log file attachment"""
    if recipients is None:
        recipients = DEFAULT_RECIPIENTS

    if sender is None:
        sender = SMTP_USERNAME if SMTP_USERNAME else DEFAULT_SENDER

    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = f"Edison SAF Import Report - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        # Create email body
        status = "SUCCESS" if error_count == 0 else "COMPLETED WITH ERRORS" if successful_imports > 0 else "FAILED"

        body = f"""Edison SAF Import Report
{'='*50}

Import Status: {status}
Completed: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Summary:
- Total imports attempted: {total_imports}
- Successful imports: {successful_imports}
- Failed imports: {total_imports - successful_imports}
- Errors encountered: {error_count}

Container: {CONTAINER_NAME}
Source: {BASE_EXPORT_PATH}
Target collections: {list(COLLECTIONS.keys())}

Detailed logs are attached to this email.

---
This is an automated report from Edison SAF Import script.
"""

        msg.attach(MIMEText(body, 'plain'))

        # Attach log file
        if os.path.exists(log_filename):
            with open(log_filename, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())

            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {os.path.basename(log_filename)}'
            )
            msg.attach(part)

        # Send email
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)

        # Only use authentication if username/password are provided
        if SMTP_USERNAME and SMTP_PASSWORD:
            if SMTP_PORT == 587 or SMTP_PORT == 465:
                server.starttls()  # Use TLS for secure ports
            server.login(SMTP_USERNAME, SMTP_PASSWORD)

        server.sendmail(sender, recipients, msg.as_string())
        server.quit()

        logger.info(f"Email report sent successfully to: {', '.join(recipients)}")
        return True

    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        return False


# Error tracking
error_messages = []

# Global logger (will be initialized in main)
logger = None

# Hardcoded Configuration
BASE_EXPORT_PATH = "/opt/edison_exports"
CONTAINER_NAME = "dspace8563"  # adjust if needed
CONTAINER_BASE_PATH = "/tmp/edison_exports"
MAPFILE_SAVE_PATH = "/tmp/mapfiles"  # mounted folder to save mapfiles

EPERSON = "dspace.admin.dev@dataquest.sk"

# Email Configuration (adjust these settings)
SMTP_SERVER = "dev-5.pc"  # local SMTP server
SMTP_PORT = 25
SMTP_USERNAME = None  # not required for local SMTP
SMTP_PASSWORD = None  # not required for local SMTP
DEFAULT_SENDER = "dspace.admin.dev@dataquest.sk"  # sender email for local SMTP
DEFAULT_RECIPIENTS = ["admin@yourdomain.com"]  # default recipients

COLLECTIONS = {
    "test": "1720d6fa-6ce9-4ee5-8b5e-fe632896e8f5",
    "test2": "9e3cd77b-fa19-4047-aca0-fa7b9bf07e36"
}


def run_command(command, description=""):
    """Run a command and return the result"""
    logger.info(f"[RUNNING] {description}")
    logger.info(f"Command: {command}")

    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        logger.info(f"Return code: {result.returncode}")
        if result.stdout:
            logger.info(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"STDERR:\n{result.stderr}")
            if result.returncode != 0:
                error_msg = f"{description} failed: {result.stderr}"
                error_messages.append(error_msg)

        return result
    except subprocess.TimeoutExpired:
        error_msg = f"{description} timed out after 5 minutes"
        logger.error("[ERROR] Command timed out after 5 minutes")
        error_messages.append(error_msg)
        return None
    except Exception as e:
        error_msg = f"{description} failed: {str(e)}"
        logger.error(f"[ERROR] Error running command: {e}")
        error_messages.append(error_msg)


def copy_data_to_container():
    """Copy Edison exports into the container"""
    logger.info("\n[COPY] Copying Edison exports to container...")

    # First ensure the base path doesn't exist in container
    cleanup_cmd = f"docker exec {CONTAINER_NAME} rm -rf {CONTAINER_BASE_PATH}"
    run_command(cleanup_cmd, "Cleaning up any existing data in container")

    # Copy data into container
    copy_cmd = f"docker cp {BASE_EXPORT_PATH} {CONTAINER_NAME}:{os.path.dirname(CONTAINER_BASE_PATH)}/"
    result = run_command(copy_cmd, "Copying data to container")

    if result and result.returncode == 0:
        logger.info("[SUCCESS] Data copied to container")
        return True
    else:
        error_msg = "Failed to copy data to container"
        logger.error(f"[ERROR] {error_msg}")
        error_messages.append(error_msg)
        return False


def find_export_directories():
    """Find export directories inside the container"""
    logger.info("\n[SEARCH] Finding export directories in container...")

    # List contents of the exports directory inside container
    list_cmd = f"docker exec {CONTAINER_NAME} find {CONTAINER_BASE_PATH} -maxdepth 1 -type d -name 'data_theses_*' -o -name 'data_dissertations_*'"
    result = run_command(list_cmd, "Listing export directories")

    if result and result.returncode == 0 and result.stdout:
        directories = [line.strip()
                       for line in result.stdout.strip().split('\n') if line.strip()]
        logger.info(f"Found {len(directories)} export directories:")
        for dir_name in directories:
            logger.info(f"  - {dir_name}")
        return directories
    else:
        error_msg = "No export directories found or error occurred"
        logger.error(f"[ERROR] {error_msg}")
        error_messages.append(error_msg)
        return []


def find_collection_directories(export_dir):
    """Find collection directories within an export directory"""
    logger.info(f"\n[SEARCH] Finding collections in {export_dir}")

    list_cmd = f"docker exec {CONTAINER_NAME} find {export_dir} -maxdepth 1 -type d"
    result = run_command(list_cmd, f"Listing contents of {export_dir}")

    if result and result.returncode == 0 and result.stdout:
        all_dirs = [line.strip()
                    for line in result.stdout.strip().split('\n') if line.strip()]
        # Filter out the parent directory itself
        collection_dirs = [d for d in all_dirs if d != export_dir]

        # Extract just the directory names and match against COLLECTIONS
        valid_collections = []
        for full_path in collection_dirs:
            dir_name = os.path.basename(full_path)
            if dir_name in COLLECTIONS:
                valid_collections.append((dir_name, full_path, COLLECTIONS[dir_name]))
                logger.info(
                    f"  [FOUND] Collection: {dir_name} -> {COLLECTIONS[dir_name]}")
            else:
                logger.info(f"  [SKIP] Unknown collection: {dir_name}")

        return valid_collections
    else:
        error_msg = f"Could not list contents of {export_dir}"
        logger.error(f"[ERROR] {error_msg}")
        error_messages.append(error_msg)
        return []


def run_dspace_import(collection_name, collection_path, collection_uuid):
    """Run DSpace SAF import for a collection"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    mapfile_path = f"{MAPFILE_SAVE_PATH}/mapfile_{collection_name}_{timestamp}.txt"

    logger.info(f"\n[IMPORT] Importing collection: {collection_name}")
    logger.info(f"  Collection UUID: {collection_uuid}")
    logger.info(f"  Source path: {collection_path}")
    logger.info(f"  Mapfile: {mapfile_path}")

    # Ensure mapfile directory exists
    create_dir_cmd = f"docker exec {CONTAINER_NAME} mkdir -p {MAPFILE_SAVE_PATH}"
    run_command(create_dir_cmd, "Creating mapfile directory")

    import_cmd = f"""docker exec -it {CONTAINER_NAME} \\
  /dspace/bin/dspace import --add \\
  --collection={collection_uuid} \\
  --source={collection_path} \\
  --eperson={EPERSON} \\
  --mapfile={mapfile_path}"""

    result = run_command(import_cmd, f"DSpace import for {collection_name}")

    logger.info(f"\n[RESULTS] Import Results for {collection_name}:")
    logger.info(f"  Export: {os.path.basename(os.path.dirname(collection_path))}")
    logger.info(f"  Collection: {collection_name}")
    logger.info(f"  Return code: {result.returncode if result else 'N/A'}")

    if result and result.returncode == 0:
        logger.info("  [SUCCESS] Import successful")
    else:
        error_msg = f"Import failed for collection {collection_name}"
        logger.error(f"  [FAILED] {error_msg}")
        error_messages.append(error_msg)

    return result


def cleanup_container():
    """Clean up copied data from container"""
    logger.info("\n[CLEANUP] Cleaning up container...")

    # Remove the copied data
    cleanup_data_cmd = f"docker exec {CONTAINER_NAME} rm -rf {CONTAINER_BASE_PATH}"
    run_command(cleanup_data_cmd, "Removing copied data from container")

    logger.info("[SUCCESS] Cleanup complete - mapfiles are saved in mounted folder")


def main():
    """Main execution function"""
    # Declare global variables at the top
    global logger, SMTP_USERNAME, SMTP_PASSWORD, SMTP_SERVER, SMTP_PORT, DEFAULT_SENDER

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Edison SAF Import to DSpace')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Show log messages on screen (default: log to file only)')
    parser.add_argument('--email', action='store_true',
                        help='Send email report with summary and log file')
    parser.add_argument('--email-to', nargs='+',
                        help='Email recipients (default: configured recipients)')
    parser.add_argument('--email-from',
                        help='Sender email address (default: dspace.admin.dev@dataquest.sk)')
    parser.add_argument('--smtp-server', default=SMTP_SERVER,
                        help=f'SMTP server (default: {SMTP_SERVER})')
    parser.add_argument('--smtp-port', type=int, default=SMTP_PORT,
                        help=f'SMTP port (default: {SMTP_PORT})')
    parser.add_argument('--smtp-username',
                        help='SMTP username (optional, for authenticated SMTP)')
    parser.add_argument('--smtp-password',
                        help='SMTP password (optional, for authenticated SMTP)')
    args = parser.parse_args()

    # Override email settings if provided
    if args.smtp_username:
        SMTP_USERNAME = args.smtp_username
    if args.smtp_password:
        SMTP_PASSWORD = args.smtp_password
    if args.email_from:
        DEFAULT_SENDER = args.email_from
    if hasattr(args, 'smtp_server') and args.smtp_server != SMTP_SERVER:
        SMTP_SERVER = args.smtp_server
    if hasattr(args, 'smtp_port') and args.smtp_port != SMTP_PORT:
        SMTP_PORT = args.smtp_port

    # Setup logging based on verbosity
    logger, log_filename = setup_logging(args.verbose)

    # No validation needed - local SMTP works without authentication

    if not args.verbose:
        print(f"Edison SAF Import starting... Logs saved to: {log_filename}")
        print("Use -v or --verbose to see detailed output on screen.\n")

    logger.info("[START] Edison SAF Import to DSpace - Starting...")
    logger.info(f"Source: {BASE_EXPORT_PATH}")
    logger.info(f"Container: {CONTAINER_NAME}")
    logger.info(f"Target collections: {list(COLLECTIONS.keys())}")

    # Step 1: Copy data to container
    if not args.verbose:
        print("Step 1: Copying data to container...")

    if not copy_data_to_container():
        logger.error("[ERROR] Failed to copy data. Exiting.")
        if not args.verbose:
            print("ERROR: Failed to copy data. Check log file for details.")
        return 1

    try:
        # Step 2: Find export directories
        if not args.verbose:
            print("Step 2: Finding export directories...")

        export_dirs = find_export_directories()
        if not export_dirs:
            logger.error("[ERROR] No export directories found. Exiting.")
            if not args.verbose:
                print("ERROR: No export directories found. Check log file for details.")
            return 1

        # Count total collections for progress tracking
        total_collections = 0
        all_collections = []

        for export_dir in export_dirs:
            collections = find_collection_directories(export_dir)
            total_collections += len(collections)
            all_collections.extend([(export_dir, col) for col in collections])

        if not args.verbose:
            print(f"Found {total_collections} collections to process\n")

        total_imports = 0
        successful_imports = 0
        current_progress = 0

        # Step 3: Process each collection with progress tracking
        for export_dir, (collection_name, collection_path, collection_uuid) in all_collections:
            current_progress += 1

            if not args.verbose:
                show_progress(current_progress, total_collections, collection_name)

            logger.info(f"\n{'='*60}")
            logger.info(f"Processing export: {os.path.basename(export_dir)}")
            logger.info(f"Processing collection: {collection_name}")
            logger.info(f"{'='*60}")

            total_imports += 1
            result = run_dspace_import(collection_name, collection_path, collection_uuid)
            if result and result.returncode == 0:
                successful_imports += 1

        # Clear progress line and show completion
        if not args.verbose:
            print("\n\nImport process completed!\n")

        # Print final summary
        logger.info(f"\n{'='*60}")
        logger.info("[SUMMARY] FINAL SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total imports attempted: {total_imports}")
        logger.info(f"Successful imports: {successful_imports}")
        logger.info(f"Failed imports: {total_imports - successful_imports}")

        # Show summary on screen for non-verbose mode
        if not args.verbose:
            print("=" * 50)
            print("FINAL SUMMARY")
            print("=" * 50)
            print(f"Total imports attempted: {total_imports}")
            print(f"Successful imports: {successful_imports}")
            print(f"Failed imports: {total_imports - successful_imports}")

        # Add error messages summary
        if error_messages:
            logger.error(f"Error messages ({len(error_messages)} errors):")
            for i, error in enumerate(error_messages, 1):
                logger.error(f"  {i}. {error}")

            if not args.verbose:
                print(
                    f"\nErrors encountered: {len(error_messages)} (see log file for details)")

        if successful_imports == total_imports:
            logger.info("[SUCCESS] All imports completed successfully!")
            if not args.verbose:
                print("\n✓ All imports completed successfully!")
        elif successful_imports > 0:
            logger.warning(
                "[WARNING] Some imports completed successfully, but there were failures")
            if not args.verbose:
                print("\n⚠ Some imports completed successfully, but there were failures")
        else:
            logger.error("[ERROR] All imports failed")
            if not args.verbose:
                print("\n✗ All imports failed")

    finally:
        # Step 5: Always cleanup
        if not args.verbose:
            print("\nCleaning up...")
        cleanup_container()

    logger.info("\n[FINISHED] Edison SAF Import completed.")

    # Send email report if requested
    if args.email:
        if not args.verbose:
            print("\nSending email report...")

        recipients = args.email_to if args.email_to else DEFAULT_RECIPIENTS
        sender = args.email_from if args.email_from else DEFAULT_SENDER
        email_sent = send_email(log_filename, total_imports, successful_imports, len(
            error_messages), recipients, sender)

        if email_sent:
            if not args.verbose:
                print(f"✓ Email report sent to: {', '.join(recipients)}")
        else:
            if not args.verbose:
                print("✗ Failed to send email report (check log for details)")

    if not args.verbose:
        email_msg = " Email report sent." if args.email and 'email_sent' in locals() and email_sent else ""
        print(f"\nProcess completed.{email_msg} Full logs available at: {log_filename}")
    return 0


if __name__ == "__main__":
    exit(main())
