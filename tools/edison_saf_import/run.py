#!/usr/bin/env python3
"""
Edison SAF Import Runner

Entry point script for running Edison SAF Import.
This provides backward compatibility with the original script usage.
"""

if __name__ == "__main__":
    from .main import main
    exit(main())
