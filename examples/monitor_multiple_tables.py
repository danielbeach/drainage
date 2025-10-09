#!/usr/bin/env python3
"""
Example script for monitoring health of multiple data lake tables.

This script demonstrates how to analyze multiple tables and generate
a summary report, useful for regular health checks and monitoring.
"""

import drainage
from datetime import datetime
from typing import List, Tuple


def monitor_tables(tables: List[Tuple[str, str]], aws_region: str = "us-west-2"):
    """
    Monitor health of multiple data lake tables.
    
    Args:
        tables: List of (s3_path, table_type) tuples
        aws_region: AWS region
    
    Returns:
        List of analysis results
    """
    
    print(f"\n{'='*80}")
    print(f"Data Lake Health Monitoring")
    print(f"{'='*80}\n")
    print(f"Analyzing {len(tables)} table(s)...")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    results = []
    
    for i, (s3_path, table_type) in enumerate(tables, 1):
        print(f"[{i}/{len(tables)}] Analyzing {s3_path} ({table_type})...")
        
        try:
            if table_type.lower() == "delta":
                report = drainage.analyze_delta_lake(s3_path, aws_region=aws_region)
            elif table_type.lower() == "iceberg":
                report = drainage.analyze_iceberg(s3_path, aws_region=aws_region)
            else:
                print(f"  ⚠️  Unknown table type: {table_type}")
                continue
            
            results.append({
                "path": s3_path,
                "type": table_type,
                "health_score": report.health_score,
                "total_files": report.metrics.total_files,
                "total_size_gb": report.metrics.total_size_bytes / (1024**3),
                "unreferenced_files": len(report.metrics.unreferenced_files),
                "unreferenced_size_gb": report.metrics.unreferenced_size_bytes / (1024**3),
                "partition_count": report.metrics.partition_count,
                "recommendations": report.metrics.recommendations,
                "analysis_time": report.analysis_timestamp
            })
            
            print(f"  ✓ Health Score: {report.health_score:.1%}")
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            results.append({
                "path": s3_path,
                "type": table_type,
                "error": str(e)
            })
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"Analysis Summary")
    print(f"{'='*80}\n")
    
    # Sort by health score
    successful_results = [r for r in results if "health_score" in r]
    failed_results = [r for r in results if "error" in r]
    
    successful_results.sort(key=lambda x: x["health_score"])
    
    # Print table health summary
    if successful_results:
        print(f"Table Health Overview (sorted by health score):")
        print(f"{'─'*80}")
        print(f"{'Path':<35} {'Type':<8} {'Health':<8} {'Files':<8} {'Size(GB)':<10} {'Issues'}")
        print(f"{'─'*80}")
        
        for r in successful_results:
            health_emoji = "🟢" if r["health_score"] > 0.8 else "🟡" if r["health_score"] > 0.6 else "🔴"
            path_short = r["path"][-35:] if len(r["path"]) > 35 else r["path"]
            print(f"{path_short:<35} {r['type']:<8} {health_emoji} {r['health_score']:.1%}  "
                  f"{r['total_files']:<8} {r['total_size_gb']:<10.2f} {len(r['recommendations'])}")
        print()
    
    # Aggregated statistics
    if successful_results:
        total_files = sum(r["total_files"] for r in successful_results)
        total_size = sum(r["total_size_gb"] for r in successful_results)
        total_unreferenced = sum(r["unreferenced_files"] for r in successful_results)
        total_wasted = sum(r["unreferenced_size_gb"] for r in successful_results)
        avg_health = sum(r["health_score"] for r in successful_results) / len(successful_results)
        
        print(f"Aggregated Statistics:")
        print(f"{'─'*80}")
        print(f"  Total Tables Analyzed:    {len(successful_results)}")
        print(f"  Average Health Score:     {avg_health:.1%}")
        print(f"  Total Files:              {total_files:,}")
        print(f"  Total Size:               {total_size:.2f} GB")
        print(f"  Total Unreferenced Files: {total_unreferenced:,}")
        print(f"  Total Wasted Space:       {total_wasted:.2f} GB\n")
    
    # Tables needing attention
    unhealthy_tables = [r for r in successful_results if r["health_score"] < 0.7]
    if unhealthy_tables:
        print(f"⚠️  Tables Needing Attention:")
        print(f"{'─'*80}")
        for r in unhealthy_tables:
            print(f"\n  📍 {r['path']}")
            print(f"     Health Score: {r['health_score']:.1%}")
            if r["recommendations"]:
                print(f"     Recommendations:")
                for rec in r["recommendations"][:3]:  # Show top 3
                    print(f"       • {rec}")
        print()
    
    # Failed analyses
    if failed_results:
        print(f"❌ Failed Analyses:")
        print(f"{'─'*80}")
        for r in failed_results:
            print(f"  {r['path']}: {r['error']}")
        print()
    
    print(f"{'='*80}\n")
    
    return results


if __name__ == "__main__":
    # Example configuration
    # Modify this list with your actual table paths
    tables_to_monitor = [
        ("s3://my-bucket/warehouse/sales_data", "delta"),
        ("s3://my-bucket/warehouse/user_events", "iceberg"),
        ("s3://my-bucket/warehouse/products", "delta"),
        ("s3://my-bucket/warehouse/inventory", "iceberg"),
    ]
    
    # Run monitoring
    results = monitor_tables(tables_to_monitor, aws_region="us-west-2")
    
    # Optional: Save results to a file
    # import json
    # with open(f"health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
    #     json.dump(results, f, indent=2)

