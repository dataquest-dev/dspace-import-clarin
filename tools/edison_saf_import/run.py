#!/usr/bin/env python3
"""
Edison SAF Import Runner

Entry point script for running Edison SAF Import.
This provides backward compatibility with the original script usage.
"""

import os
import sys

if __name__ == "__main__":
    if __package__ in (None, ""):
        # Running as a script: add parent directory to sys.path and use absolute import
        package_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(package_dir)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        import edison_saf_import.main as main_module
    else:
        # Running as a module within a package: relative import is valid
        from . import main as main_module
    exit(main_module.main())
