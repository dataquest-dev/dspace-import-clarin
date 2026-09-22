"""The remove_authority tool, imported by path, plus what its two test files share."""
import importlib.util
import logging
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TOOL_FILE = os.path.join(ROOT_DIR, "tools", "remove_authority", "remove_authority.py")

AUTHOR = "dc.contributor.author"


def _load():
    spec = importlib.util.spec_from_file_location("remove_authority", TOOL_FILE)
    module = importlib.util.module_from_spec(spec)
    sys.modules["remove_authority"] = module
    spec.loader.exec_module(module)
    return module


tool = _load()
# dspace_rest_client runs logging.basicConfig(level=INFO) when it is imported
logging.getLogger().setLevel(logging.WARNING)


def value(val, authority=None, confidence=-1, place=0, language=None):
    """One metadata value as the REST API returns it."""
    return {"value": val, "language": language, "authority": authority,
            "confidence": confidence, "place": place}
