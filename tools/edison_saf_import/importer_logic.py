"""
Core import logic for Edison SAF Import to DSpace.

Contains the main EdisonImporter class that handles Docker operations,
DSpace imports, and progress tracking.
"""

import datetime
import logging
import os
import subprocess
from typing import List, Optional, Tuple

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

from . import config


class ProgressTracker:
    """Handles progress display for import operations using tqdm."""

    def __init__(self, total: int, desc: str = "Processing"):
        """Initialize progress tracker.

        Args:
            total: Total number of items to process
            desc: Description for the progress bar
        """
        self.total = total
        if tqdm:
            self.pbar = tqdm(
                total=total,
                desc=desc,
                unit="collection",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] - {postfix}"
            )
        else:
            # Fallback to simple counter if tqdm not available
            self.pbar = None
            self.current = 0

    def update(self, collection_name: str = "") -> None:
        """Update and display progress."""
        if self.pbar:
            self.pbar.set_postfix_str(f"Processing: {collection_name}")
            self.pbar.update(1)
        else:
            # Fallback display
            self.current += 1
            print(
                f"Progress: {self.current}/{self.total} - Processing: {collection_name}")

    def finish(self) -> None:
        """Close progress bar."""
        if self.pbar:
            self.pbar.close()
        else:
            print()


class EdisonImporter:
    """Manages Edison SAF import operations to DSpace via Docker."""

    def __init__(self, verbose: bool = False, logger: Optional[logging.Logger] = None):
        """Initialize Edison importer.

        Args:
            verbose: Whether to show verbose output
            logger: Logger instance (optional)
        """
        self.verbose = verbose
        self.logger = logger or logging.getLogger(__name__)
        self.error_messages: List[str] = []

    def run_command(self, command: str, description: str = "") -> Optional[subprocess.CompletedProcess]:
        """Execute a shell command with proper error handling.

        Args:
            command: Shell command to execute
            description: Description of the command for logging

        Returns:
            CompletedProcess result or None if timeout/error
        """
        self.logger.info(f"[RUNNING] {description}")
        self.logger.info(f"Command: {command}")

        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=config.COMMAND_TIMEOUT,
                check=False
            )

            self.logger.info(f"Return code: {result.returncode}")
            if result.stdout:
                self.logger.info(f"STDOUT:\n{result.stdout}")
            if result.stderr:
                self.logger.warning(f"STDERR:\n{result.stderr}")
                if result.returncode != 0:
                    error_msg = f"{description} failed: {result.stderr}"
                    self.error_messages.append(error_msg)

            return result

        except subprocess.TimeoutExpired:
            error_msg = f"{description} timed out after {config.COMMAND_TIMEOUT} seconds"
            self.logger.error(
                f"[ERROR] Command timed out after {config.COMMAND_TIMEOUT} seconds")
            self.error_messages.append(error_msg)
            return None
        except Exception as e:
            error_msg = f"{description} failed: {str(e)}"
            self.logger.error(f"[ERROR] Error running command: {e}")
            self.error_messages.append(error_msg)
            return None

    def copy_data_to_container(self) -> bool:
        """Copy Edison exports into the container.

        Returns:
            True if successful, False otherwise
        """
        self.logger.info("\n[COPY] Copying Edison exports to container...")

        # Clean up any existing data
        cleanup_cmd = f"docker exec {config.CONTAINER_NAME} rm -rf {config.CONTAINER_BASE_PATH}"
        self.run_command(cleanup_cmd, "Cleaning up any existing data in container")

        # Copy data into container
        copy_cmd = f"docker cp {config.BASE_EXPORT_PATH} {config.CONTAINER_NAME}:{os.path.dirname(config.CONTAINER_BASE_PATH)}/"
        result = self.run_command(copy_cmd, "Copying data to container")

        if result and result.returncode == 0:
            self.logger.info("[SUCCESS] Data copied to container")
            return True
        else:
            error_msg = "Failed to copy data to container"
            self.logger.error(f"[ERROR] {error_msg}")
            self.error_messages.append(error_msg)
            return False

    def find_export_directories(self) -> List[str]:
        """Find export directories inside the container.

        Returns:
            List of export directory paths
        """
        self.logger.info("\n[SEARCH] Finding export directories in container...")

        # Build the find command with all patterns from config.EXPORT_DIR_PATTERNS
        pattern_args = ' '.join(f"-name '{pat}'" for pat in config.EXPORT_DIR_PATTERNS)
        # Join patterns with -o (OR)
        pattern_expr = ' -o '.join(
            [f"-name '{pat}'" for pat in config.EXPORT_DIR_PATTERNS])
        list_cmd = (
            f"docker exec {config.CONTAINER_NAME} find {config.CONTAINER_BASE_PATH} "
            f"-maxdepth 1 -type d {pattern_expr}"
        )
        result = self.run_command(list_cmd, "Listing export directories")

        if result and result.returncode == 0 and result.stdout:
            directories = [line.strip()
                           for line in result.stdout.strip().split('\n') if line.strip()]
            self.logger.info(f"Found {len(directories)} export directories:")
            for dir_name in directories:
                self.logger.info(f"  - {dir_name}")
            return directories
        else:
            error_msg = "No export directories found or error occurred"
            self.logger.error(f"[ERROR] {error_msg}")
            self.error_messages.append(error_msg)
            return []

    def find_collection_directories(self, export_dir: str) -> List[Tuple[str, str, str]]:
        """Find collection directories within an export directory.

        Args:
            export_dir: Path to export directory

        Returns:
            List of tuples: (collection_name, full_path, collection_uuid)
        """
        self.logger.info(f"\n[SEARCH] Finding collections in {export_dir}")

        list_cmd = f"docker exec {config.CONTAINER_NAME} find {export_dir} -maxdepth 1 -type d"
        result = self.run_command(list_cmd, f"Listing contents of {export_dir}")

        if result and result.returncode == 0 and result.stdout:
            all_dirs = [line.strip()
                        for line in result.stdout.strip().split('\n') if line.strip()]
            collection_dirs = [d for d in all_dirs if d != export_dir]

            valid_collections = []
            for full_path in collection_dirs:
                dir_name = os.path.basename(full_path)
                if dir_name in config.COLLECTIONS:
                    valid_collections.append(
                        (dir_name, full_path, config.COLLECTIONS[dir_name]))
                    self.logger.info(
                        f"  [FOUND] Collection: {dir_name} -> {config.COLLECTIONS[dir_name]}")
                else:
                    self.logger.info(f"  [SKIP] Unknown collection: {dir_name}")

            return valid_collections
        else:
            error_msg = f"Could not list contents of {export_dir}"
            self.logger.error(f"[ERROR] {error_msg}")
            self.error_messages.append(error_msg)
            return []

    def run_dspace_import(
        self,
        collection_name: str,
        collection_path: str,
        collection_uuid: str
    ) -> Optional[subprocess.CompletedProcess]:
        """Run DSpace SAF import for a collection.

        Args:
            collection_name: Name of the collection
            collection_path: Path to collection data
            collection_uuid: UUID of the target collection

        Returns:
            CompletedProcess result or None if failed
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        mapfile_path = f"{config.MAPFILE_SAVE_PATH}/mapfile_{collection_name}_{timestamp}.txt"

        self.logger.info(f"\n[IMPORT] Importing collection: {collection_name}")
        self.logger.info(f"  Collection UUID: {collection_uuid}")
        self.logger.info(f"  Source path: {collection_path}")
        self.logger.info(f"  Mapfile: {mapfile_path}")

        # Ensure mapfile directory exists
        create_dir_cmd = f"docker exec {config.CONTAINER_NAME} mkdir -p {config.MAPFILE_SAVE_PATH}"
        self.run_command(create_dir_cmd, "Creating mapfile directory")

        import_cmd = (f"docker exec -it {config.CONTAINER_NAME} "
                      f"/dspace/bin/dspace import --add "
                      f"--collection={collection_uuid} "
                      f"--source={collection_path} "
                      f"--eperson={config.EPERSON} "
                      f"--mapfile={mapfile_path}")

        result = self.run_command(import_cmd, f"DSpace import for {collection_name}")

        self.logger.info(f"\n[RESULTS] Import Results for {collection_name}:")
        self.logger.info(
            f"  Export: {os.path.basename(os.path.dirname(collection_path))}")
        self.logger.info(f"  Collection: {collection_name}")
        self.logger.info(f"  Return code: {result.returncode if result else 'N/A'}")

        if result and result.returncode == 0:
            self.logger.info("  [SUCCESS] Import successful")
        else:
            error_msg = f"Import failed for collection {collection_name}"
            self.logger.error(f"  [FAILED] {error_msg}")
            self.error_messages.append(error_msg)

        return result

    def cleanup_container(self) -> None:
        """Clean up copied data from container."""
        self.logger.info("\n[CLEANUP] Cleaning up container...")

        cleanup_data_cmd = f"docker exec {config.CONTAINER_NAME} rm -rf {config.CONTAINER_BASE_PATH}"
        self.run_command(cleanup_data_cmd, "Removing copied data from container")

        self.logger.info(
            "[SUCCESS] Cleanup complete - mapfiles are saved in mounted folder")

    def process_imports(self) -> Tuple[int, int]:
        """Process all imports with progress tracking.

        Returns:
            Tuple of (total_imports, successful_imports)
        """
        export_dirs = self.find_export_directories()
        if not export_dirs:
            self.logger.error("[ERROR] No export directories found")
            return 0, 0

        # Collect all collections to process
        all_collections = []
        for export_dir in export_dirs:
            collections = self.find_collection_directories(export_dir)
            all_collections.extend([(export_dir, col) for col in collections])

        if not all_collections:
            self.logger.error("[ERROR] No valid collections found")
            return 0, 0

        total_collections = len(all_collections)
        progress = ProgressTracker(
            total_collections, "Importing collections") if not self.verbose else None

        self.logger.info(f"Found {total_collections} collections to process\n", extra={
                         'summary': True})

        total_imports = 0
        successful_imports = 0

        try:
            for export_dir, (collection_name, collection_path, collection_uuid) in all_collections:
                if progress:
                    progress.update(collection_name)

                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"Processing export: {os.path.basename(export_dir)}")
                self.logger.info(f"Processing collection: {collection_name}")
                self.logger.info(f"{'='*60}")

                total_imports += 1
                result = self.run_dspace_import(
                    collection_name, collection_path, collection_uuid)
                if result and result.returncode == 0:
                    successful_imports += 1

        finally:
            if progress:
                progress.finish()

        return total_imports, successful_imports
