# defender_report/cli.py

"""
CLI entry-point for the Defender Report tool.
"""

import sys
from .main import main

if __name__ == "__main__":
    # return a proper exit code
    sys.exit(main())
