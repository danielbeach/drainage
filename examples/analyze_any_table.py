#!/usr/bin/env python3
"""
Example script for analyzing any data lake table with automatic type detection.

This script demonstrates the analyze_table() function which automatically
detects whether a table is Delta Lake or Apache Iceberg and runs the
appropriate analysis.
"""

import sys
import drainage


def analyze_any_table(
    s3_path: str, table_type: str = None, aws_region: str = "us-west-2"
):
    """
    Analyze any data lake table with automatic type detection.

    Args:
        s3_path: S3 path to the table (e.g., s3://bucket/path/to/table)
        table_type: Optional table type ("delta" or "iceberg"). If None, auto-detects.
        aws_region: AWS region (defaults to us-west-2)
    """

    print(f"\n{'='*70}")
    print("Analyzing Data Lake Table")
    print(f"{'='*70}\n")
    print(f"📍 Location: {s3_path}")
    print(f"🌎 Region: {aws_region}")
    if table_type:
        print(f"🏷️  Type: {table_type} (explicitly specified)")
    else:
        print("🔍 Type: Auto-detection enabled")
    print("\nAnalyzing... This may take a few moments...\n")

    try:
        # Run the analysis with optional type specification
        report = drainage.analyze_table(
            s3_path=s3_path, table_type=table_type, aws_region=aws_region
        )

        # Print header
        print(f"{'='*70}")
        print("Analysis Complete!")
        print(f"{'='*70}\n")

        # Overall health score
        health_emoji = (
            "🟢"
            if report.health_score > 0.8
            else "🟡"
            if report.health_score > 0.6
            else "🔴"
        )
        print(f"{health_emoji} Overall Health Score: {report.health_score:.1%}")
        print(f"📅 Analysis Timestamp: {report.analysis_timestamp}")
        print(f"🏷️  Detected Type: {report.table_type}\n")

        # Key metrics
        print("📊 Key Metrics:")
        print(f"{'─'*70}")
        print(f"  Total Files:         {report.metrics.total_files:,}")

        # Format size in GB or MB
        size_gb = report.metrics.total_size_bytes / (1024**3)
        if size_gb >= 1:
            print(f"  Total Size:          {size_gb:.2f} GB")
        else:
            size_mb = report.metrics.total_size_bytes / (1024**2)
            print(f"  Total Size:          {size_mb:.2f} MB")

        # Average file size
        avg_mb = report.metrics.avg_file_size_bytes / (1024**2)
        print(f"  Average File Size:   {avg_mb:.2f} MB")
        print(f"  Partition Count:     {report.metrics.partition_count:,}\n")

        # File size distribution
        print("📦 File Size Distribution:")
        print(f"{'─'*70}")
        dist = report.metrics.file_size_distribution
        total_files = (
            dist.small_files
            + dist.medium_files
            + dist.large_files
            + dist.very_large_files
        )

        if total_files > 0:
            small_pct = dist.small_files / total_files * 100
            print(
                f"  Small (<16MB):       {dist.small_files:>6} files "
                f"({small_pct:>5.1f}%)"
            )
            medium_pct = dist.medium_files / total_files * 100
            print(
                f"  Medium (16-128MB):   {dist.medium_files:>6} files "
                f"({medium_pct:>5.1f}%)"
            )
            large_pct = dist.large_files / total_files * 100
            print(
                f"  Large (128MB-1GB):   {dist.large_files:>6} files "
                f"({large_pct:>5.1f}%)"
            )
            very_large_pct = dist.very_large_files / total_files * 100
            print(
                f"  Very Large (>1GB):   {dist.very_large_files:>6} files "
                f"({very_large_pct:>5.1f}%)\n"
            )

        # Clustering information (Iceberg only)
        if report.metrics.clustering:
            print("🎯 Clustering Information:")
            print(f"{'─'*70}")
            clustering = report.metrics.clustering
            print(f"  Clustering Columns:  {', '.join(clustering.clustering_columns)}")
            print(f"  Cluster Count:       {clustering.cluster_count:,}")
            print(f"  Avg Files/Cluster:   {clustering.avg_files_per_cluster:.2f}")
            cluster_size_mb = clustering.avg_cluster_size_bytes / (1024**2)
            print(f"  Avg Cluster Size:    {cluster_size_mb:.2f} MB\n")

        # Unreferenced files warning
        if report.metrics.unreferenced_files:
            print("⚠️  Unreferenced Files:")
            print(f"{'─'*70}")
            print(f"  Count:  {len(report.metrics.unreferenced_files):,}")
            wasted_gb = report.metrics.unreferenced_size_bytes / (1024**3)
            if wasted_gb >= 1:
                print(f"  Wasted: {wasted_gb:.2f} GB")
            else:
                wasted_mb = report.metrics.unreferenced_size_bytes / (1024**2)
                print(f"  Wasted: {wasted_mb:.2f} MB")

            table_type_name = (
                "Delta transaction log"
                if report.table_type == "delta"
                else "Iceberg manifest files"
            )
            print("\n  These files exist in S3 but are not referenced in the")
            print(f"  {table_type_name}. Consider cleaning them up.\n")

        # Recommendations
        if report.metrics.recommendations:
            print("💡 Recommendations:")
            print(f"{'─'*70}")
            for i, rec in enumerate(report.metrics.recommendations, 1):
                print(f"  {i}. {rec}")
            print()
        else:
            print("✅ No recommendations - table is in excellent health!\n")

        print(f"{'='*70}\n")

        return report

    except Exception as e:
        print(f"\n❌ Error analyzing table: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    # Example usage
    if len(sys.argv) < 2:
        print("Usage: python analyze_any_table.py <s3_path> [table_type] [aws_region]")
        print("\nExamples:")
        print("  # Auto-detect table type")
        print("  python analyze_any_table.py s3://my-bucket/my-table us-west-2")
        print("  # Specify table type explicitly")
        print(
            "  python analyze_any_table.py s3://my-bucket/my-delta-table delta "
            "us-west-2"
        )
        print(
            "  python analyze_any_table.py s3://my-bucket/my-iceberg-table "
            "iceberg us-west-2"
        )
        sys.exit(1)

    s3_path = sys.argv[1]
    table_type = sys.argv[2] if len(sys.argv) > 2 else None
    aws_region = sys.argv[3] if len(sys.argv) > 3 else "us-west-2"

    analyze_any_table(s3_path, table_type, aws_region)
