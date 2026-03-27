import sys
import os
import logging

_this_dir = os.path.dirname(os.path.abspath(__file__))
path_to_dspace_lib = os.path.join(_this_dir, "../../libs/dspace-rest-python")
sys.path.insert(0, path_to_dspace_lib)
sys.path.insert(0, os.path.join(_this_dir, "../../src"))
import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from utils import init_logging, update_settings  # noqa
_logger = logging.getLogger()

env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])

if __name__ == "__main__":
    _logger.info("Started...")

    dspace_be = dspace.rest(env["backend"]["endpoint"], env["backend"]["user"], env["backend"]["password"], True)
    for i in range(1, 10):
        _logger.info(f"Authenticating {i}")
        dspace_be.client.authenticate()

        # Call logout every 5th request
        # if i % 5 == 0:
        #     dspace_be.client.logout()
        #     print("Logged out")
