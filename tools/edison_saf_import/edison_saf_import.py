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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            f'/tmp/edison_import_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Error tracking
error_messages = []

# Hardcoded Configuration
BASE_EXPORT_PATH = "/opt/edison_exports"
CONTAINER_NAME = "dspace8563"  # adjust if needed
CONTAINER_BASE_PATH = "/tmp/edison_exports"
MAPFILE_SAVE_PATH = "/tmp/mapfiles"  # mounted folder to save mapfiles

EPERSON = "dspace.admin.dev@dataquest.sk"

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
    logger.info("[START] Edison SAF Import to DSpace - Starting...")
    logger.info(f"Source: {BASE_EXPORT_PATH}")
    logger.info(f"Container: {CONTAINER_NAME}")
    logger.info(f"Target collections: {list(COLLECTIONS.keys())}")

    # Step 1: Copy data to container
    if not copy_data_to_container():
        logger.error("[ERROR] Failed to copy data. Exiting.")
        return 1

    try:
        # Step 2: Find export directories
        export_dirs = find_export_directories()
        if not export_dirs:
            logger.error("[ERROR] No export directories found. Exiting.")
            return 1

        total_imports = 0
        successful_imports = 0

        # Step 3: Process each export directory
        for export_dir in export_dirs:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing export: {os.path.basename(export_dir)}")
            logger.info(f"{'='*60}")

            collections = find_collection_directories(export_dir)

            # Step 4: Import each collection
            for collection_name, collection_path, collection_uuid in collections:
                total_imports += 1
                result = run_dspace_import(
                    collection_name, collection_path, collection_uuid)
                if result and result.returncode == 0:
                    successful_imports += 1

        # Print final summary
        logger.info(f"\n{'='*60}")
        logger.info("[SUMMARY] FINAL SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total imports attempted: {total_imports}")
        logger.info(f"Successful imports: {successful_imports}")
        logger.info(f"Failed imports: {total_imports - successful_imports}")

        # Add error messages summary
        if error_messages:
            logger.error(f"Error messages ({len(error_messages)} errors):")
            for i, error in enumerate(error_messages, 1):
                logger.error(f"  {i}. {error}")

        if successful_imports == total_imports:
            logger.info("[SUCCESS] All imports completed successfully!")
        elif successful_imports > 0:
            logger.warning(
                "[WARNING] Some imports completed successfully, but there were failures")
        else:
            logger.error("[ERROR] All imports failed")

    finally:
        # Step 5: Always cleanup
        cleanup_container()

    logger.info("\n[FINISHED] Edison SAF Import completed.")
    return 0


if __name__ == "__main__":
    exit(main())
