#!/usr/bin/env python3
"""
Simple example showing the easiest way to analyze a data lake table.

This script demonstrates the most straightforward usage of the drainage library
with the built-in print_health_report function.
"""

import sys
import drainage


def main():
    """Simple table analysis with built-in formatting."""

    if len(sys.argv) < 2:
        print("Usage: python simple_analysis.py <s3_path> [aws_region]")
        print("\nExample:")
        print("  python simple_analysis.py s3://my-bucket/my-table us-west-2")
        sys.exit(1)

    s3_path = sys.argv[1]
    aws_region = sys.argv[2] if len(sys.argv) > 2 else "us-west-2"

    print(f"Analyzing table: {s3_path}")
    print(f"Region: {aws_region}")
    print("This may take a few moments...\n")

    try:
        # Analyze the table (auto-detects type)
        report = drainage.analyze_table(s3_path, aws_region=aws_region)

        # Print the comprehensive health report
        drainage.print_health_report(report)

        # You can also access individual metrics if needed
        print("\nQuick Summary:")
        print(f"  Health Score: {report.health_score:.1%}")
        print(f"  Table Type: {report.table_type}")
        print(f"  Total Files: {report.metrics.total_files:,}")
        print(f"  Unreferenced Files: {len(report.metrics.unreferenced_files)}")

    except Exception as e:
        print(f"\n❌ Error analyzing table: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
