"""
Configuration settings for Edison SAF Import.

This module contains all configuration constants, collection mappings,
and environment-dependent variables.
"""


import os
from typing import Dict

# Directory paths
BASE_EXPORT_PATH = "/opt/edison_exports"
CONTAINER_NAME = "dspace8563"
CONTAINER_BASE_PATH = "/tmp/edison_exports"
MAPFILE_SAVE_PATH = "/tmp/mapfiles"


# DSpace configuration
EPERSON = os.environ.get("EDISON_SAF_IMPORT_EPERSON",
                         "dspace.admin.dev@dataquest.sk")  # CHANGE THIS for production

# Email configuration
SMTP_SERVER = os.environ.get("EDISON_SAF_IMPORT_SMTP_SERVER",
                             "dev-5.pc")  # CHANGE THIS for production
SMTP_PORT = int(os.environ.get("EDISON_SAF_IMPORT_SMTP_PORT", 25))
SMTP_USERNAME = os.environ.get("EDISON_SAF_IMPORT_SMTP_USERNAME")
SMTP_PASSWORD = os.environ.get("EDISON_SAF_IMPORT_SMTP_PASSWORD")
# Sender and recipients should be customized for your deployment!
DEFAULT_SENDER = os.environ.get(
    "EDISON_SAF_IMPORT_SENDER", "dspace.admin.dev@dataquest.sk")  # CHANGE THIS for production
DEFAULT_RECIPIENTS = os.environ.get(
    "EDISON_SAF_IMPORT_RECIPIENTS", "admin@yourdomain.com").split(",")  # Comma-separated list


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
