#!/usr/bin/env python3
"""
Deprecated: Create clustered Delta helper

AWS S3 support has been removed from this project. The helper scripts
that created S3-backed test tables are deprecated. Use ADLS-based
examples in `examples/` and the `delta_skelter` ADLS client instead.
"""

import sys


def main():
    print(
        "This helper is deprecated: AWS S3 support removed. See README.md for ADLS examples."
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
