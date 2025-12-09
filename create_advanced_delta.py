#!/usr/bin/env python3
"""
Deprecated: AWS S3 helpers

This script previously created a Delta Lake table in S3 for testing.
The project has removed AWS S3 support and replaced it with Azure ADLS.
Maintain the file as a small deprecation stub so users know what changed.
"""

import sys


def main():
    print(
        "This script is deprecated: AWS S3 support removed. Use ADLS examples instead."
    )
    print("See README.md for ADLS + UAMI usage examples.")
    sys.exit(1)


if __name__ == "__main__":
    main()
