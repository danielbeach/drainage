# Drainage 🌊

╔══════════════════════════════════════════════════════════╗
║   🌊  D R A I N A G E  🦀                                 ║
║   Rust + Python Lake House Health Analyzer               ║
║   Detect • Diagnose • Optimize • Flow                    ║
╚══════════════════════════════════════════════════════════╝
                         ~~~~~     ~~~~~~


A high-performance Rust library with Python bindings for analyzing the health of remote S3-stored data lakes (Delta Lake and Apache Iceberg). Drainage helps you understand and optimize your data lake by identifying issues like unreferenced files, suboptimal partitioning, and inefficient file sizes.

## Features

- **🚀 Fast Analysis**: Built in Rust for maximum performance
- **📊 Comprehensive Health Metrics**: 
  - Unreferenced and orphaned data files detection
  - Partition and clustering analysis (Delta Lake liquid clustering + Iceberg clustering)
  - File size distribution and optimization recommendations
  - Data skew analysis (partition and file size skew)
  - Metadata health monitoring
  - Snapshot retention analysis
  - **Deletion vector impact analysis** (Delta Lake & Iceberg v3+)
  - **Schema evolution stability tracking** (Delta Lake & Iceberg)
  - **Time travel storage cost analysis** (Delta Lake & Iceberg)
  - **Table constraints and data quality insights** (Delta Lake & Iceberg)
  - **Advanced file compaction optimization** (Delta Lake & Iceberg)
  - Overall health score calculation
- **🔍 Multi-Format Support**:
  - **Delta Lake tables** (including liquid clustering support)
  - **Apache Iceberg tables** (including clustering support)
- **☁️ S3 Native**: Direct S3 integration for analyzing remote data lakes
- **🐍 Python Interface**: Easy-to-use Python API powered by PyO3

## Installation

### As a Python Package

```bash
pip install drainage
```

### From Source

```bash
# Install maturin for building Rust Python extensions
pip install maturin

# Build and install the package
cd drainage
maturin develop --release
```

## Quick Start

### Quick Analysis (Auto-Detection)

```python
import drainage

# Analyze any table with automatic type detection
report = drainage.analyze_table(
    s3_path="s3://my-bucket/my-table",
    aws_region="us-west-2"
)

# Print a comprehensive health report
drainage.print_health_report(report)

# Or access individual metrics
print(f"Health Score: {report.health_score}")
print(f"Table Type: {report.table_type}")
print(f"Total Files: {report.metrics.total_files}")
```

### Analyzing a Delta Lake Table

```python
import drainage

# Analyze a Delta Lake table
report = drainage.analyze_delta_lake(
    s3_path="s3://my-bucket/my-delta-table",
    aws_access_key_id="YOUR_ACCESS_KEY",  # Optional if using IAM roles
    aws_secret_access_key="YOUR_SECRET_KEY",  # Optional if using IAM roles
    aws_region="us-west-2"  # Optional, defaults to us-east-1
)

# View the health score (0.0 to 1.0)
print(f"Health Score: {report.health_score}")

# Check metrics
print(f"Total Files: {report.metrics.total_files}")
print(f"Total Size: {report.metrics.total_size_bytes} bytes")
print(f"Unreferenced Files: {len(report.metrics.unreferenced_files)}")
print(f"Partition Count: {report.metrics.partition_count}")

# View recommendations
for recommendation in report.metrics.recommendations:
    print(f"⚠️  {recommendation}")
```

### Analyzing an Apache Iceberg Table

```python
import drainage

# Analyze an Apache Iceberg table
report = drainage.analyze_iceberg(
    s3_path="s3://my-bucket/my-iceberg-table",
    aws_region="us-west-2"
)

# View file size distribution
dist = report.metrics.file_size_distribution
print(f"Small files (<16MB): {dist.small_files}")
print(f"Medium files (16-128MB): {dist.medium_files}")
print(f"Large files (128MB-1GB): {dist.large_files}")
print(f"Very large files (>1GB): {dist.very_large_files}")
```
## Working on Databricks
```
import drainage
# Alternative: Use the table location directly
tables = spark.sql("SHOW TABLES IN development.backend_dev")
for table in tables.collect():
    table_name = table['tableName']
    
    # Get table properties including location
    table_info = spark.sql(f"DESCRIBE TABLE EXTENDED development.backend_dev.{table_name}")
    
    # Look for Location in the output
    location_rows = table_info.filter(table_info['col_name'] == 'Location').collect()
    
    if location_rows:
        s3_path = location_rows[0]['data_type']
        print(f"Table: {table_name}, Location: {s3_path}")
        
        # Test if this is a valid Delta Lake table
        if s3_path.startswith("s3://") and "__unitystorage" in s3_path:
            print(f"✅ Unity Catalog Delta table: {s3_path}")
            # Try analysis
            try:
                report = drainage.analyze_table(s3_path=s3_path, aws_region="us-east-1")
                drainage.print_health_report(report)
            except Exception as e:
                print(f"❌ Analysis failed: {e}")
```


## Health Metrics Explained

### Health Score

The health score ranges from 0.0 (poor health) to 1.0 (excellent health) and is calculated based on:

- **Unreferenced Files** (-30%): Files that exist in S3 but aren't referenced in table metadata
- **Small Files** (-20%): High percentage of small files (<16MB) indicates inefficient storage
- **Very Large Files** (-10%): Files over 1GB may cause performance issues
- **Partitioning** (-10-15%): Too many or too few files per partition
- **Data Skew** (-15-25%): Uneven data distribution across partitions and file sizes
- **Metadata Bloat** (-5%): Large metadata files that slow down operations
- **Snapshot Retention** (-10%): Too many historical snapshots affecting performance
- **Deletion Vector Impact** (-15%): High deletion vector impact affecting query performance
- **Schema Instability** (-20%): Unstable schema evolution affecting compatibility and performance
- **Time Travel Storage Costs** (-10%): High time travel storage costs affecting budget
- **Data Quality Issues** (-15%): Poor data quality from insufficient constraints
- **File Compaction Opportunities** (-10%): Missed compaction opportunities affecting performance

### Key Metrics

#### File Analysis
- `total_files`: Total number of data files in the table
- `total_size_bytes`: Total size of all data files
- `avg_file_size_bytes`: Average file size
- `unreferenced_files`: List of files not referenced in table metadata
- `unreferenced_size_bytes`: Total size of unreferenced files

#### Partition Analysis
- `partition_count`: Number of partitions
- `partitions`: Detailed information about each partition including:
  - Partition values
  - File count per partition
  - Total and average file sizes
  
#### File Size Distribution
- `small_files`: Files under 16MB
- `medium_files`: Files between 16MB and 128MB
- `large_files`: Files between 128MB and 1GB
- `very_large_files`: Files over 1GB

#### Clustering (Delta Lake & Iceberg)
- `clustering_columns`: Columns used for clustering/sorting
- `cluster_count`: Number of clusters
- `avg_files_per_cluster`: Average files per cluster
- **Delta Lake**: Supports liquid clustering (up to 4 columns)
- **Iceberg**: Supports traditional clustering and Z-order

#### Data Skew Analysis
- `partition_skew_score`: How unevenly data is distributed across partitions (0.0 = perfect, 1.0 = highly skewed)
- `file_size_skew_score`: Variation in file sizes within partitions
- `largest_partition_size`: Size of the largest partition
- `smallest_partition_size`: Size of the smallest partition
- `avg_partition_size`: Average partition size
- `partition_size_std_dev`: Standard deviation of partition sizes

#### Metadata Health
- `metadata_file_count`: Number of transaction logs/manifest files
- `metadata_total_size_bytes`: Combined size of all metadata files
- `avg_metadata_file_size`: Average size of metadata files
- `metadata_growth_rate`: Estimated metadata growth rate
- `manifest_file_count`: Number of manifest files (Iceberg only)

#### Snapshot Health
- `snapshot_count`: Number of historical snapshots
- `oldest_snapshot_age_days`: Age of the oldest snapshot
- `newest_snapshot_age_days`: Age of the newest snapshot
- `avg_snapshot_age_days`: Average snapshot age
- `snapshot_retention_risk`: Risk level based on snapshot count (0.0 = good, 1.0 = high risk)

#### Deletion Vector Analysis (Delta Lake & Iceberg v3+)
- `deletion_vector_count`: Number of deletion vectors
- `total_deletion_vector_size_bytes`: Total size of all deletion vectors
- `avg_deletion_vector_size_bytes`: Average deletion vector size
- `deletion_vector_age_days`: Age of the oldest deletion vector
- `deleted_rows_count`: Total number of deleted rows
- `deletion_vector_impact_score`: Performance impact score (0.0 = no impact, 1.0 = high impact)

#### Schema Evolution Tracking (Delta Lake & Iceberg)
- `total_schema_changes`: Total number of schema changes
- `breaking_changes`: Number of breaking schema changes
- `non_breaking_changes`: Number of non-breaking schema changes
- `schema_stability_score`: Schema stability score (0.0 = unstable, 1.0 = very stable)
- `days_since_last_change`: Days since last schema change
- `schema_change_frequency`: Schema changes per day
- `current_schema_version`: Current schema version

#### Time Travel Analysis (Delta Lake & Iceberg)
- `total_snapshots`: Total number of historical snapshots
- `oldest_snapshot_age_days`: Age of the oldest snapshot in days
- `newest_snapshot_age_days`: Age of the newest snapshot in days
- `total_historical_size_bytes`: Total size of all historical snapshots
- `avg_snapshot_size_bytes`: Average size of snapshots
- `storage_cost_impact_score`: Storage cost impact score (0.0 = low cost, 1.0 = high cost)
- `retention_efficiency_score`: Retention efficiency score (0.0 = inefficient, 1.0 = very efficient)
- `recommended_retention_days`: Recommended retention period in days

#### Table Constraints Analysis (Delta Lake & Iceberg)
- `total_constraints`: Total number of table constraints
- `check_constraints`: Number of check constraints
- `not_null_constraints`: Number of NOT NULL constraints
- `unique_constraints`: Number of unique constraints
- `foreign_key_constraints`: Number of foreign key constraints
- `constraint_violation_risk`: Risk of constraint violations (0.0 = low risk, 1.0 = high risk)
- `data_quality_score`: Data quality score based on constraints (0.0 = poor quality, 1.0 = excellent quality)
- `constraint_coverage_score`: Constraint coverage score (0.0 = no coverage, 1.0 = full coverage)

#### File Compaction Analysis (Delta Lake & Iceberg)
- `compaction_opportunity_score`: Compaction opportunity score (0.0 = no opportunity, 1.0 = high opportunity)
- `small_files_count`: Number of small files (<16MB)
- `small_files_size_bytes`: Total size of small files
- `potential_compaction_files`: Number of files that could be compacted
- `estimated_compaction_savings_bytes`: Estimated storage savings from compaction
- `recommended_target_file_size_bytes`: Recommended target file size for compaction
- `compaction_priority`: Compaction priority level (low, medium, high, critical)
- `z_order_opportunity`: Whether Z-ordering would be beneficial
- `z_order_columns`: Columns recommended for Z-ordering

### Recommendations

Drainage automatically generates recommendations based on the analysis:

- **Orphaned Files**: Suggests cleanup of unreferenced files
- **Small Files**: Recommends compaction to improve query performance
- **Large Files**: Suggests splitting for better parallelism
- **Partition Issues**: Advises on repartitioning strategy
- **Clustering Issues**: Recommends clustering optimization (Delta Lake liquid clustering, Iceberg clustering)
- **Data Skew**: Recommends repartitioning or file reorganization to balance data distribution
- **Metadata Bloat**: Suggests running VACUUM (Delta) or expire_snapshots (Iceberg) to clean up metadata
- **Snapshot Retention**: Advises on snapshot cleanup to improve performance
- **Deletion Vector Issues**: Recommends VACUUM (Delta) or expire_snapshots (Iceberg) to clean up old deletion vectors
- **Schema Evolution Issues**: Advises on schema change planning and batching to improve stability
- **Time Travel Storage Issues**: Recommends optimizing retention policies to reduce storage costs
- **Data Quality Issues**: Suggests adding table constraints to improve data quality
- **File Compaction Issues**: Recommends OPTIMIZE (Delta) or rewrite_data_files (Iceberg) for performance
- **Z-Ordering Opportunities**: Suggests Z-ordering to improve query performance
- **Empty Partitions**: Suggests removing empty partition directories

## Examples

### Complete Analysis Script

```python
import drainage
import json

def print_health_report(report):
    """Print a comprehensive health report."""
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Table Health Report: {report.table_path}")
    print(f"Type: {report.table_type}")
    print(f"Analysis Time: {report.analysis_timestamp}")
    print(f"{'='*60}\n")
    
    print(f"🏥 Overall Health Score: {report.health_score:.2%}")
    print(f"\n📊 Key Metrics:")
    print(f"  - Total Files: {report.metrics.total_files:,}")
    print(f"  - Total Size: {report.metrics.total_size_bytes / (1024**3):.2f} GB")
    print(f"  - Average File Size: {report.metrics.avg_file_size_bytes / (1024**2):.2f} MB")
    print(f"  - Partitions: {report.metrics.partition_count:,}")
    
    # File size distribution
    print(f"\n📦 File Size Distribution:")
    dist = report.metrics.file_size_distribution
    total = dist.small_files + dist.medium_files + dist.large_files + dist.very_large_files
    if total > 0:
        print(f"  - Small (<16MB): {dist.small_files} ({dist.small_files/total*100:.1f}%)")
        print(f"  - Medium (16-128MB): {dist.medium_files} ({dist.medium_files/total*100:.1f}%)")
        print(f"  - Large (128MB-1GB): {dist.large_files} ({dist.large_files/total*100:.1f}%)")
        print(f"  - Very Large (>1GB): {dist.very_large_files} ({dist.very_large_files/total*100:.1f}%)")
    
    # Unreferenced files
    if report.metrics.unreferenced_files:
        print(f"\n⚠️  Unreferenced Files: {len(report.metrics.unreferenced_files)}")
        print(f"  - Wasted Space: {report.metrics.unreferenced_size_bytes / (1024**3):.2f} GB")
    
    # Recommendations
    if report.metrics.recommendations:
        print(f"\n💡 Recommendations:")
        for i, rec in enumerate(report.metrics.recommendations, 1):
            print(f"  {i}. {rec}")
    
    return report

# Using the built-in analyze_table function with auto-detection
report = drainage.analyze_table("s3://my-bucket/my-table", aws_region="us-west-2")
drainage.print_health_report(report)

# Or specify the table type explicitly
report = drainage.analyze_table("s3://my-bucket/my-delta-table", table_type="delta", aws_region="us-west-2")
drainage.print_health_report(report)
```

### Using Example Scripts

The `examples/` directory contains ready-to-use scripts:

#### Simple Analysis (Recommended)

```bash
python examples/simple_analysis.py s3://my-bucket/my-table us-west-2
```

#### Analyze Any Table (Auto-Detection)

```bash
python examples/analyze_any_table.py s3://my-bucket/my-table us-west-2
```

#### Analyze a Single Delta Table

```bash
python examples/analyze_delta_table.py s3://my-bucket/my-table us-west-2
```

#### Analyze a Single Iceberg Table

```bash
python examples/analyze_iceberg_table.py s3://my-bucket/my-table us-west-2
```

#### Monitor Multiple Tables

```bash
python examples/monitor_multiple_tables.py
```

### Monitoring Multiple Tables

```python
import drainage
from datetime import datetime

tables = [
    ("s3://bucket/sales_data", "delta"),
    ("s3://bucket/user_events", "iceberg"),
    ("s3://bucket/products", "delta"),
]

results = []

for s3_path, table_type in tables:
    try:
        if table_type == "delta":
            report = drainage.analyze_delta_lake(s3_path, aws_region="us-west-2")
        else:
            report = drainage.analyze_iceberg(s3_path, aws_region="us-west-2")
        
        results.append({
            "path": s3_path,
            "type": table_type,
            "health_score": report.health_score,
            "total_files": report.metrics.total_files,
            "unreferenced_files": len(report.metrics.unreferenced_files),
            "recommendations": len(report.metrics.recommendations)
        })
    except Exception as e:
        print(f"Error analyzing {s3_path}: {e}")

# Sort by health score
results.sort(key=lambda x: x["health_score"])

print("\nTable Health Summary (sorted by health score):")
print(f"{'Path':<40} {'Type':<8} {'Health':<8} {'Files':<10} {'Issues':<8}")
print("-" * 85)
for r in results:
    print(f"{r['path']:<40} {r['type']:<8} {r['health_score']:.2%}  {r['total_files']:<10} {r['recommendations']:<8}")
```

## Sample Output

Here's what a comprehensive health report looks like with all the new advanced metrics:

```
============================================================
Table Health Report: s3://my-bucket/my-delta-table
Type: delta
Analysis Time: 2025-01-27T10:30:00Z
============================================================

🟢 Overall Health Score: 85.2%

📊 Key Metrics:
────────────────────────────────────────────────────────────
  Total Files:         1,234
  Total Size:          2.45 GB
  Average File Size:   2.03 MB
  Partition Count:     12

📦 File Size Distribution:
────────────────────────────────────────────────────────────
  Small (<16MB):         45 files ( 3.6%)
  Medium (16-128MB):   1,156 files (93.7%)
  Large (128MB-1GB):      33 files ( 2.7%)
  Very Large (>1GB):       0 files ( 0.0%)

🎯 Clustering Information:
────────────────────────────────────────────────────────────
  Clustering Columns:  department, age
  Cluster Count:       12
  Avg Files/Cluster:   102.83
  Avg Cluster Size:    204.17 MB

📊 Data Skew Analysis:
────────────────────────────────────────────────────────────
  Partition Skew Score: 0.23 (0=perfect, 1=highly skewed)
  File Size Skew:       0.15 (0=perfect, 1=highly skewed)
  Largest Partition:    245.67 MB
  Smallest Partition:   12.34 MB
  Avg Partition Size:   89.45 MB

📋 Metadata Health:
────────────────────────────────────────────────────────────
  Metadata Files:       15
  Metadata Size:        2.34 MB
  Avg Metadata File:    0.16 MB

📸 Snapshot Health:
────────────────────────────────────────────────────────────
  Snapshot Count:       15
  Retention Risk:       20.0%

🗑️  Deletion Vector Analysis:
────────────────────────────────────────────────────────────
  Deletion Vectors:     3
  Total DV Size:        1.2 MB
  Deleted Rows:         1,456
  Oldest DV Age:        5.2 days
  Impact Score:         0.15 (0=no impact, 1=high impact)

📋 Schema Evolution Analysis:
────────────────────────────────────────────────────────────
  Total Changes:         8
  Breaking Changes:      1
  Non-Breaking Changes:  7
  Stability Score:       0.85 (0=unstable, 1=very stable)
  Days Since Last:       12.3 days
  Change Frequency:      0.15 changes/day
  Current Version:       8

⏰ Time Travel Analysis:
────────────────────────────────────────────────────────────
  Total Snapshots:       45
  Oldest Snapshot:       15.2 days
  Newest Snapshot:       0.1 days
  Historical Size:       1.2 GB
  Storage Cost Impact:   0.25 (0=low cost, 1=high cost)
  Retention Efficiency:  0.85 (0=inefficient, 1=very efficient)
  Recommended Retention: 30 days

🔒 Table Constraints Analysis:
────────────────────────────────────────────────────────────
  Total Constraints:     12
  Check Constraints:     3
  NOT NULL Constraints:  8
  Unique Constraints:    1
  Foreign Key Constraints: 0
  Violation Risk:        0.15 (0=low risk, 1=high risk)
  Data Quality Score:    0.92 (0=poor quality, 1=excellent quality)
  Constraint Coverage:   0.75 (0=no coverage, 1=full coverage)

📦 File Compaction Analysis:
────────────────────────────────────────────────────────────
  Compaction Opportunity: 0.85 (0=no opportunity, 1=high opportunity)
  Small Files Count:     23
  Small Files Size:      45.2 MB
  Potential Compaction:  23 files
  Estimated Savings:     12.8 MB
  Recommended Target:    128 MB
  Compaction Priority:   HIGH
  Z-Order Opportunity:   Yes
  Z-Order Columns:       department, age, created_date

⚠️  Unreferenced Files:
────────────────────────────────────────────────────────────
  Count:  5
  Wasted: 12.3 MB

  These files exist in S3 but are not referenced in the
  Delta transaction log. Consider cleaning them up.

💡 Recommendations:
────────────────────────────────────────────────────────────
  1. Found 5 unreferenced files (12.3 MB). Consider cleaning up orphaned data files.
  2. High percentage of small files detected. Consider compacting to improve query performance.
  3. Old deletion vectors detected. Consider running VACUUM to clean up deletion vectors older than 30 days.
  4. Recent schema changes detected. Monitor query performance for potential issues.
  5. High file compaction opportunity detected. Consider running OPTIMIZE to improve performance.
  6. Z-ordering opportunity detected. Consider running OPTIMIZE ZORDER BY (department, age, created_date) to improve query performance.
  7. Significant compaction savings available: 12.8 MB. Consider running OPTIMIZE.

============================================================
```

## Architecture

Drainage is built with:

- **Rust Core**: High-performance analysis engine
- **PyO3**: Seamless Python-Rust integration
- **AWS SDK**: Native S3 integration
- **Tokio**: Async runtime for concurrent operations

The library analyzes table metadata (Delta transaction logs, Iceberg manifests) and compares it against actual S3 objects to identify issues and provide optimization recommendations.

## Development

### Building

```bash
# Development build
maturin develop

# Release build
maturin develop --release

# Build wheel
maturin build --release
```

### Testing

```bash
# Run Rust tests
cargo test

# Run with example
python examples/simple_analysis.py s3://your-bucket/your-table
```

## Performance

Drainage is designed for speed:

- ⚡ Async I/O for concurrent S3 operations
- 🦀 Rust performance for data processing
- 📦 Efficient memory usage for large tables

Typical analysis times:
- Small tables (<1000 files): < 5 seconds
- Medium tables (1000-10000 files): 10-30 seconds
- Large tables (>10000 files): 30-120 seconds

## Roadmap

- [ ] Support for Hudi tables
- [ ] Automated repair/optimization actions
- [ ] Detailed partition skew analysis
- [ ] Query performance prediction
- [ ] Web dashboard for monitoring
- [ ] CloudWatch/Datadog integration
- [ ] Table comparison and diff

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details
