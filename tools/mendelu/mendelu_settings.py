import os
from datetime import datetime

_this_dir = os.path.dirname(os.path.abspath(__file__))
ts = datetime.now().strftime("%Y_%m_%d__%H.%M.%S")

settings = {
    "log_file": os.path.join(_this_dir, "__logs", f"{ts}.txt"),

    "backend": {
        "endpoint": os.environ.get("DSPACE_ENDPOINT", "http://localhost:8080/server/api"),
        "user": os.environ.get("DSPACE_USER", ""),
        "password": os.environ.get("DSPACE_PASSWORD", ""),
        "authentication": True,
    },

    "dspace": {
        "handle_prefix": ["http://hdl.handle.net/", "https://hdl.handle.net/"],
        "orcid_url": "https://orcid.org/{}",
    },

    "update_type": {
        "mapping_file": os.path.join(_this_dir, "update_type", "mapping.json"),
    },
}

