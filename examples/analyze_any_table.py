#!/usr/bin/env python3
"""
Example script for analyzing any data lake table with automatic type detection.

This script demonstrates the analyze_table() function which analyzes
Delta Lake tables (Apache Iceberg is not supported and will raise
NotImplementedError).
"""

import sys
import delta_skelter


def analyze_any_table(path: str, table_type: str = None, client_id: str = None):
    """
    Analyze any data lake table with automatic type detection.

    Args:
        path: ADLS path to the table (e.g., abfss://<filesystem>@<account>.dfs.core.windows.net/<prefix>)
        table_type: Optional table type ("delta"). If None, defaults to Delta.
        client_id: Optional user-assigned managed identity client id (UAMI)
    """

    print(f"\n{'='*70}")
    print("Analyzing Data Lake Table")
    print(f"{'='*70}\n")
    print(f"📍 Location: {path}")
    if client_id:
        print(f"🔐 Using UAMI client id: {client_id}")
    if table_type:
        print(f"🏷️  Type: {table_type} (explicitly specified)")
    else:
        print("🔍 Type: Auto-detection enabled")
    print("\nAnalyzing... This may take a few moments...\n")

    try:
        # Run the analysis (Delta Lake only)
        report = delta_skelter.analyze_table(path, table_type or "delta", client_id)

        # Print header
        print(f"{'='*70}")
        print("Analysis Complete!")
        print(f"{'='*70}\n")

        # Overall health score
        health_emoji = (
            "🟢"
            if report.health_score > 0.8
            else "🟡" if report.health_score > 0.6 else "🔴"
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

        # Clustering information (if present)
        if getattr(report.metrics, "clustering", None):
            print("🎯 Clustering Information:")
            print(f"{'─'*70}")
            clustering = report.metrics.clustering
            # clustering may be a dict in the Python implementation
            cols = getattr(clustering, "clustering_columns", None) or clustering.get(
                "clustering_columns", []
            )
            print(f"  Clustering Columns:  {', '.join(cols)}")
            print(f"  Cluster Count:       {clustering.get('cluster_count', 0):,}")
            print(
                f"  Avg Files/Cluster:   {clustering.get('avg_files_per_cluster', 0.0):.2f}"
            )

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

            table_type_name = "Delta transaction log"
            print("\n  These files exist in storage but are not referenced in the")
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
        print(
            "Usage: python analyze_any_table.py <adls_path> [table_type] [uami_client_id]"
        )
        print("\nExamples:")
        print("  # Auto-detect table type")
        print(
            "  python analyze_any_table.py abfss://fs@account.dfs.core.windows.net/my-table"
        )
        print("  # Specify table type explicitly")
        print(
            "  python analyze_any_table.py abfss://fs@account.dfs.core.windows.net/my-delta-table delta"
        )
        print(
            "  python analyze_any_table.py abfss://fs@account.dfs.core.windows.net/my-delta-table delta <uami-client-id>"
        )
        sys.exit(1)

    path = sys.argv[1]
    table_type = sys.argv[2] if len(sys.argv) > 2 else None
    client_id = sys.argv[3] if len(sys.argv) > 3 else None

    analyze_any_table(path, table_type, client_id)
