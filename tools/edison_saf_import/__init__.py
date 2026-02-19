"""
Edison SAF Import to DSpace

A modular Python package for importing Edison exports into DSpace via Docker.
"""


# Import config as _config and re-export public names
from . import config as _config
__all__ = getattr(
    _config,
    "__all__",
    [name for name in dir(_config) if not name.startswith("_")],
)
for name in __all__:
    globals()[name] = getattr(_config, name)

__version__ = "2.0.0"
__author__ = "Dataquest"
