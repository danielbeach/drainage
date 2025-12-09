#!/usr/bin/env python3
"""
Example script for analyzing a Delta Lake table health.

This script demonstrates how to use the drainage library to analyze
a Delta Lake table stored on ADLS and get comprehensive health metrics.
"""

import sys
import drainage


def analyze_delta_table(path: str, client_id: str = None):
    """
    Analyze a Delta Lake table and print comprehensive health report.

    Args:
        path: ADLS path to the Delta table (e.g., abfss://<filesystem>@<account>.dfs.core.windows.net/<prefix>)
        client_id: Optional user-assigned managed identity client id (UAMI)
    """

    print(f"\n{'='*70}")
    print("Analyzing Delta Lake Table")
    print(f"{'='*70}\n")
    print(f"📍 Location: {path}")
    if client_id:
        print(f"🔐 Using UAMI client id: {client_id}")
    print("\nAnalyzing... This may take a few moments...\n")

    try:
        # Run the analysis
        report = drainage.analyze_delta_lake(path, client_id)

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
        print(f"📅 Analysis Timestamp: {report.analysis_timestamp}\n")

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
            medium_pct = dist.medium_files / total_files * 100
            large_pct = dist.large_files / total_files * 100
            very_large_pct = dist.very_large_files / total_files * 100

            print(
                f"  Small (<16MB):       {dist.small_files:>6} files "
                f"({small_pct:>5.1f}%)"
            )
            print(
                f"  Medium (16-128MB):   {dist.medium_files:>6} files "
                f"({medium_pct:>5.1f}%)"
            )
            print(
                f"  Large (128MB-1GB):   {dist.large_files:>6} files "
                f"({large_pct:>5.1f}%)"
            )
            print(
                f"  Very Large (>1GB):   {dist.very_large_files:>6} files "
                f"({very_large_pct:>5.1f}%)\n"
            )

        # Partition analysis
        if report.metrics.partitions:
            print("🗂️  Partition Analysis:")
            print(f"{'─'*70}")
            print(f"  Total Partitions: {len(report.metrics.partitions):,}")

            # Show top 5 largest partitions
            sorted_partitions = sorted(
                report.metrics.partitions,
                key=lambda p: p.total_size_bytes,
                reverse=True,
            )[:5]

            if sorted_partitions:
                print("\n  Top 5 Largest Partitions:")
                for i, part in enumerate(sorted_partitions, 1):
                    part_size_mb = part.total_size_bytes / (1024**2)
                    avg_file_mb = part.avg_file_size_bytes / (1024**2)
                    print(
                        f"    {i}. Files: {part.file_count:>4}, "
                        f"Size: {part_size_mb:>8.2f} MB, "
                        f"Avg: {avg_file_mb:>6.2f} MB"
                    )
            print()

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

            print("\n  These files exist in storage but are not referenced in the")
            print("  Delta transaction log. Consider cleaning them up.\n")

        # Recommendations
        if report.metrics.recommendations:
            print("💡 Recommendations:")
            print(f"{'─'*70}")
            for i, rec in enumerate(report.metrics.recommendations, 1):
                print(f"  {i}. {rec}")
            print()
        else:
            print("✅ No recommendations - table is in good health!\n")

        print(f"{'='*70}\n")

        return report

    except Exception as e:
        print(f"\n❌ Error analyzing table: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    # Example usage
    if len(sys.argv) < 2:
        print("Usage: python analyze_delta_table.py <adls_path> [uami_client_id]")
        print("\nExample:")
        print(
            "  python analyze_delta_table.py abfss://fs@account.dfs.core.windows.net/my-delta-table"
        )
        sys.exit(1)

    path = sys.argv[1]
    client_id = sys.argv[2] if len(sys.argv) > 2 else None

    analyze_delta_table(path, client_id)
