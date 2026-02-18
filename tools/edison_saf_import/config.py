"""
Configuration settings for Edison SAF Import.

This module contains all configuration constants, collection mappings,
and environment-dependent variables.
"""

from typing import Dict

# Directory paths
BASE_EXPORT_PATH = "/opt/edison_exports"
CONTAINER_NAME = "dspace8563"
CONTAINER_BASE_PATH = "/tmp/edison_exports"
MAPFILE_SAVE_PATH = "/tmp/mapfiles"

# DSpace configuration
EPERSON = "dspace.admin.dev@dataquest.sk"

# Email configuration
SMTP_SERVER = "dev-5.pc"
SMTP_PORT = 25
SMTP_USERNAME = None
SMTP_PASSWORD = None
DEFAULT_SENDER = "dspace.admin.dev@dataquest.sk"
DEFAULT_RECIPIENTS = ["admin@yourdomain.com"]


# Collection mappings: collection_name -> uuid
COLLECTIONS: Dict[str, str] = {
    "test": "1720d6fa-6ce9-4ee5-8b5e-fe632896e8f5",
    "test2": "9e3cd77b-fa19-4047-aca0-fa7b9bf07e36"
}

# Export directory patterns (configurable)
EXPORT_DIR_PATTERNS = [
    'data_theses_*',
    'data_dissertations_*',
]

# Command timeouts (in seconds)
COMMAND_TIMEOUT = 300
