#!/usr/bin/env python3
"""
Edison SAF Import to DSpace - POC Script

This script copies Edison exports from /opt/edison_exports into a Docker container,
runs DSpace SAF import for each collection, and cleans up afterwards.
"""

import subprocess
import os
import datetime

# Hardcoded Configuration
BASE_EXPORT_PATH = "/opt/edison_exports"
CONTAINER_NAME = "dspace8563"  # adjust if needed
CONTAINER_BASE_PATH = "/tmp/edison_exports"

EPERSON = "dspace.admin.dev@dataquest.sk"

COLLECTIONS = {
    "test": "1720d6fa-6ce9-4ee5-8b5e-fe632896e8f5",
    "test2": "9e3cd77b-fa19-4047-aca0-fa7b9bf07e36"
}


def run_command(command, description=""):
    """Run a command and return the result"""
    print(f"\n[RUNNING] {description}")
    print(f"Command: {command}")

    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        print(f"Return code: {result.returncode}")
        if result.stdout:
            print(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            print(f"STDERR:\n{result.stderr}")

        return result
    except subprocess.TimeoutExpired:
        print("[ERROR] Command timed out after 5 minutes")
        return None
    except Exception as e:
        print(f"[ERROR] Error running command: {e}")
        return None


def copy_data_to_container():
    """Copy Edison exports into the container"""
    print("\n[COPY] Copying Edison exports to container...")

    # First ensure the base path doesn't exist in container
    cleanup_cmd = f"docker exec {CONTAINER_NAME} rm -rf {CONTAINER_BASE_PATH}"
    run_command(cleanup_cmd, "Cleaning up any existing data in container")

    # Copy data into container
    copy_cmd = f"docker cp {BASE_EXPORT_PATH} {CONTAINER_NAME}:{CONTAINER_BASE_PATH}/"
    result = run_command(copy_cmd, "Copying data to container")

    if result and result.returncode == 0:
        print("[SUCCESS] Data copied to container")
        return True
    else:
        print("[ERROR] Failed to copy data to container")
        return False


def find_export_directories():
    """Find export directories inside the container"""
    print("\n[SEARCH] Finding export directories in container...")

    # List contents of the exports directory inside container
    list_cmd = f"docker exec {CONTAINER_NAME} find {CONTAINER_BASE_PATH} -maxdepth 1 -type d -name 'data_theses_*' -o -name 'data_dissertations_*'"
    result = run_command(list_cmd, "Listing export directories")

    if result and result.returncode == 0 and result.stdout:
        directories = [line.strip()
                       for line in result.stdout.strip().split('\n') if line.strip()]
        print(f"Found {len(directories)} export directories:")
        for dir_name in directories:
            print(f"  - {dir_name}")
        return directories
    else:
        print("[ERROR] No export directories found or error occurred")
        return []


def find_collection_directories(export_dir):
    """Find collection directories within an export directory"""
    print(f"\n[SEARCH] Finding collections in {export_dir}")

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
                print(f"  [FOUND] Collection: {dir_name} -> {COLLECTIONS[dir_name]}")
            else:
                print(f"  [SKIP] Unknown collection: {dir_name}")

        return valid_collections
    else:
        print(f"[ERROR] Could not list contents of {export_dir}")
        return []


def run_dspace_import(collection_name, collection_path, collection_uuid):
    """Run DSpace SAF import for a collection"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    mapfile_path = f"/tmp/mapfile_{collection_name}_{timestamp}.txt"

    print(f"\n[IMPORT] Importing collection: {collection_name}")
    print(f"  Collection UUID: {collection_uuid}")
    print(f"  Source path: {collection_path}")
    print(f"  Mapfile: {mapfile_path}")

    import_cmd = f"""docker exec -it {CONTAINER_NAME} \\
  /dspace/bin/dspace import --add \\
  --collection={collection_uuid} \\
  --source={collection_path} \\
  --eperson={EPERSON} \\
  --mapfile={mapfile_path}"""

    result = run_command(import_cmd, f"DSpace import for {collection_name}")

    print(f"\n[RESULTS] Import Results for {collection_name}:")
    print(f"  Export: {os.path.basename(os.path.dirname(collection_path))}")
    print(f"  Collection: {collection_name}")
    print(f"  Return code: {result.returncode if result else 'N/A'}")

    if result and result.returncode == 0:
        print("  [SUCCESS] Import successful")
    else:
        print("  [FAILED] Import failed")

    return result


def cleanup_container():
    """Clean up copied data and temporary files from container"""
    print("\n[CLEANUP] Cleaning up container...")

    # Remove the copied data
    cleanup_data_cmd = f"docker exec {CONTAINER_NAME} rm -rf {CONTAINER_BASE_PATH}"
    run_command(cleanup_data_cmd, "Removing copied data from container")

    # Remove any mapfiles
    cleanup_maps_cmd = f"docker exec {CONTAINER_NAME} bash -c 'rm -f /tmp/mapfile_*.txt'"
    run_command(cleanup_maps_cmd, "Removing temporary mapfiles")

    print("[SUCCESS] Cleanup complete")


def main():
    """Main execution function"""
    print("[START] Edison SAF Import to DSpace - Starting...")
    print(f"Source: {BASE_EXPORT_PATH}")
    print(f"Container: {CONTAINER_NAME}")
    print(f"Target collections: {list(COLLECTIONS.keys())}")

    # Step 1: Copy data to container
    if not copy_data_to_container():
        print("[ERROR] Failed to copy data. Exiting.")
        return 1

    try:
        # Step 2: Find export directories
        export_dirs = find_export_directories()
        if not export_dirs:
            print("[ERROR] No export directories found. Exiting.")
            return 1

        total_imports = 0
        successful_imports = 0

        # Step 3: Process each export directory
        for export_dir in export_dirs:
            print(f"\n{'='*60}")
            print(f"Processing export: {os.path.basename(export_dir)}")
            print(f"{'='*60}")

            collections = find_collection_directories(export_dir)

            # Step 4: Import each collection
            for collection_name, collection_path, collection_uuid in collections:
                total_imports += 1
                result = run_dspace_import(
                    collection_name, collection_path, collection_uuid)
                if result and result.returncode == 0:
                    successful_imports += 1

        # Print final summary
        print(f"\n{'='*60}")
        print("[SUMMARY] FINAL SUMMARY")
        print(f"{'='*60}")
        print(f"Total imports attempted: {total_imports}")
        print(f"Successful imports: {successful_imports}")
        print(f"Failed imports: {total_imports - successful_imports}")

        if successful_imports == total_imports:
            print("[SUCCESS] All imports completed successfully!")
        elif successful_imports > 0:
            print("[WARNING] Some imports completed successfully, but there were failures")
        else:
            print("[ERROR] All imports failed")

    finally:
        # Step 5: Always cleanup
        cleanup_container()

    print("\n[FINISHED] Edison SAF Import completed.")
    return 0


if __name__ == "__main__":
    exit(main())
