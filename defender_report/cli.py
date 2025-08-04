"""
CLI entry-point for the Defender Report tool.
"""

import sys
from defender_report.main import main

def cli_entry():
    try:
        result = main()
        if isinstance(result, int):
            return result
        return 0
    except KeyboardInterrupt:
        print("\nAborted by user.")
        return 130
    except Exception as exc:
        print(f"\n[ERROR] Unexpected exception: {exc}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(cli_entry())
