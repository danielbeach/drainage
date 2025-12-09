# Delta-Skelter 🌊

Heavily inspired by and initially forked from the drainage project, this is a python-only project that analyses the health of Delta Lake tables stored on Azure Data Lake Storage (ADLS).

Delta-Skelter helps you understand and optimize your data lake by identifying issues like unreferenced files, suboptimal partitioning, and inefficient file sizes.

## Features

- **🚀 Fast Analysis**: Efficient Python implementation for broad portability
- **📊 Comprehensive Health Metrics**: 
  - Unreferenced and orphaned data files detection
  - Partition and clustering analysis (Delta Lake liquid clustering)
  - File size distribution and optimization recommendations
  - Data skew analysis (partition and file size skew)
  - Metadata health monitoring
  - Snapshot retention analysis
  - **Deletion vector impact analysis** (Delta Lake)
  - **Schema evolution stability tracking** (Delta Lake)
  - **Time travel storage cost analysis** (Delta Lake)
  - **Table constraints and data quality insights** (Delta Lake)
  - **Advanced file compaction optimization** (Delta Lake)
  - Overall health score calculation
- **🔍 Multi-Format Support**:
  - **Delta Lake tables** (including liquid clustering support)
  - **☁️ ADLS Native**: Direct integration with Azure Data Lake Storage (supports UAMI)
  - **🐍 Python Interface**: Easy-to-use Python API
- **🧪 Comprehensive Testing**: Full test suite with CI/CD across multiple platforms

## Installation

### From Source

```bash
# Install editable package from source
pip install -e .

# Or build a wheel and install
python -m build
pip install dist/*.whl
```

## Quick Start

### Quick Analysis (Auto-Detection)

```python
import delta_skelter

# Analyze any table (Delta Lake on ADLS) with automatic detection
report = delta_skelter.analyze_table("abfss://myfs@account.dfs.core.windows.net/my-table")

# Print a comprehensive health report
delta_skelter.print_health_report(report)

# Or access individual metrics
print(f"Health Score: {report.health_score}")
print(f"Table Type: {report.table_type}")
print(f"Total Files: {report.metrics.total_files}")
```

### Analyzing a Delta Lake Table

```python
import delta_skelter

# Analyze a Delta Lake table on ADLS
report = delta_skelter.analyze_delta_lake("abfss://myfs@account.dfs.core.windows.net/my-delta-table")

print(f"Health Score: {report.health_score}")
print(f"Total Files: {report.metrics.total_files}")
print(f"Total Size: {report.metrics.total_size_bytes} bytes")
print(f"Unreferenced Files: {len(report.metrics.unreferenced_files)}")
```

## Health Metrics Explained

### Health Score

The health score ranges from 0.0 (poor health) to 1.0 (excellent health) and is calculated based on:

-- **Unreferenced Files** (-30%): Files that exist in the data lake but aren't referenced in table metadata
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

#### Clustering (Delta Lake)
- `clustering_columns`: Columns used for clustering/sorting
- `cluster_count`: Number of clusters
- `avg_files_per_cluster`: Average files per cluster
- **Delta Lake**: Supports liquid clustering (up to 4 columns)

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
- `avg_metadata_file_size_bytes`: Average size of metadata files
- `metadata_growth_rate`: Estimated metadata growth rate
- `manifest_file_count`: Number of manifest files (Iceberg not supported in this release)

#### Snapshot Health
- `snapshot_count`: Number of historical snapshots
- `oldest_snapshot_age_days`: Age of the oldest snapshot
- `newest_snapshot_age_days`: Age of the newest snapshot
- `avg_snapshot_age_days`: Average snapshot age
- `snapshot_retention_risk`: Risk level based on snapshot count (0.0 = good, 1.0 = high risk)

#### Deletion Vector Analysis (Delta Lake)
- `deletion_vector_count`: Number of deletion vectors
- `total_deletion_vector_size_bytes`: Total size of all deletion vectors
- `avg_deletion_vector_size_bytes`: Average deletion vector size
- `deletion_vector_age_days`: Age of the oldest deletion vector
- `deleted_rows_count`: Total number of deleted rows
- `deletion_vector_impact_score`: Performance impact score (0.0 = no impact, 1.0 = high impact)

#### Schema Evolution Tracking (Delta Lake)
- `total_schema_changes`: Total number of schema changes
- `breaking_changes`: Number of breaking schema changes
- `non_breaking_changes`: Number of non-breaking schema changes
- `schema_stability_score`: Schema stability score (0.0 = unstable, 1.0 = very stable)
- `days_since_last_change`: Days since last schema change
- `schema_change_frequency`: Schema changes per day
- `current_schema_version`: Current schema version

#### Time Travel Analysis (Delta Lake)
- `total_snapshots`: Total number of historical snapshots
- `oldest_snapshot_age_days`: Age of the oldest snapshot in days
- `newest_snapshot_age_days`: Age of the newest snapshot in days
- `total_historical_size_bytes`: Total size of all historical snapshots
- `avg_snapshot_size_bytes`: Average size of snapshots
- `storage_cost_impact_score`: Storage cost impact score (0.0 = low cost, 1.0 = high cost)
- `retention_efficiency_score`: Retention efficiency score (0.0 = inefficient, 1.0 = very efficient)
- `recommended_retention_days`: Recommended retention period in days

#### Table Constraints Analysis (Delta Lake)
- `total_constraints`: Total number of table constraints
- `check_constraints`: Number of check constraints
- `not_null_constraints`: Number of NOT NULL constraints
- `unique_constraints`: Number of unique constraints
- `foreign_key_constraints`: Number of foreign key constraints
- `constraint_violation_risk`: Risk of constraint violations (0.0 = low risk, 1.0 = high risk)
- `data_quality_score`: Data quality score based on constraints (0.0 = poor quality, 1.0 = excellent quality)
- `constraint_coverage_score`: Constraint coverage score (0.0 = no coverage, 1.0 = full coverage)

#### File Compaction Analysis (Delta Lake)
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

Delta-Skelter automatically generates recommendations based on the analysis:

- **Orphaned Files**: Suggests cleanup of unreferenced files
- **Small Files**: Recommends compaction to improve query performance
- **Large Files**: Suggests splitting for better parallelism
- **Partition Issues**: Advises on repartitioning strategy
- **Clustering Issues**: Recommends clustering optimization (Delta Lake liquid clustering)
- **Data Skew**: Recommends repartitioning or file reorganization to balance data distribution
- **Metadata Bloat**: Suggests running VACUUM (Delta) to clean up metadata
- **Snapshot Retention**: Advises on snapshot cleanup to improve performance
-- **Deletion Vector Issues**: Recommends VACUUM (Delta) to clean up old deletion vectors
- **Schema Evolution Issues**: Advises on schema change planning and batching to improve stability
- **Time Travel Storage Issues**: Recommends optimizing retention policies to reduce storage costs
- **Data Quality Issues**: Suggests adding table constraints to improve data quality
-- **File Compaction Issues**: Recommends OPTIMIZE (Delta) for performance
- **Z-Ordering Opportunities**: Suggests Z-ordering to improve query performance
- **Empty Partitions**: Suggests removing empty partition directories

## Examples

### Complete Analysis Script

```python
import delta_skelter
import json

def print_health_report(report):
    """Print a comprehensive health report."""
  - **Deletion vector impact analysis** (Delta Lake only in this release)
    # Print summary
    print(f"\n{'='*60}")
  - Apache Iceberg support removed — Delta Lake only in this release
    print(f"Type: {report.table_type}")
-- **☁️ ADLS Native**: Direct integration with Azure Data Lake Storage (supports UAMI)
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
report = delta_skelter.analyze_table("abfss://myfs@account.dfs.core.windows.net/my-table")
delta_skelter.print_health_report(report)

# Or specify the table type explicitly
report = delta_skelter.analyze_table("abfss://myfs@account.dfs.core.windows.net/my-delta-table", table_type="delta")
delta_skelter.print_health_report(report)
```

### Using Example Scripts

The `examples/` directory contains ready-to-use scripts:

#### Simple Analysis (Recommended)

```bash
python examples/simple_analysis.py abfss://myfs@account.dfs.core.windows.net/my-table
```

#### Analyze Any Table (Auto-Detection)

```bash
python examples/analyze_any_table.py abfss://myfs@account.dfs.core.windows.net/my-table
```

#### Analyze a Single Delta Table

```bash
python examples/analyze_delta_table.py abfss://myfs@account.dfs.core.windows.net/my-table
```

#### Monitor Multiple Tables

```bash
python examples/monitor_multiple_tables.py
```

### Run the UI (demo scenarios available)

1) Start the API/UI server from the repo root:

```bash
python -m uvicorn app:app --host 0.0.0.0 --port 8080
# or: python app.py
```

2) Open the UI at `http://localhost:8080/ui`.

3) For real analysis, enter an ADLS path (abfss/https) and submit.

4) For local examples, use the **Demo scenarios** buttons:
   - **Very well housekept**
   - **Quite well housekept**
   - **Extremely poorly housekept**

Each button calls the demo API with a curated dataset—no ADLS access required.

### Monitoring Multiple Tables

```python
import delta_skelter
from datetime import datetime

tables = [
  ("abfss://salesfs@account.dfs.core.windows.net/sales_data", "delta"),
  ("abfss://eventsfs@account.dfs.core.windows.net/user_events", "delta"),
  ("abfss://productsfs@account.dfs.core.windows.net/products", "delta"),
]

results = []

for path, table_type in tables:
  try:
    report = delta_skelter.analyze_table(path)

    results.append({
      "path": path,
      "type": table_type,
      "health_score": report.health_score,
      "total_files": report.metrics.total_files,
      "unreferenced_files": len(report.metrics.unreferenced_files),
      "recommendations": len(report.metrics.recommendations)
    })
    except Exception as e:
        print(f"Error analyzing {path}: {e}")

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
Table Health Report: abfss://myfs@account.dfs.core.windows.net/my-delta-table
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

  These files exist in ADLS but are not referenced in the
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

Delta-Skelter is built with:

- **Python Implementation**: Pure Python analyzer (Rust components removed)
- **Azure ADLS SDK**: Native ADLS integration via `azure-identity` and `azure-storage-file-datalake`
- **Async I/O**: Uses `asyncio` for concurrent operations

The library analyzes table metadata (Delta transaction logs) and compares it against actual ADLS objects to identify issues and provide optimization recommendations.

### Metadata health thresholds

The analyzer computes basic metadata health metrics (transaction/log file counts, total metadata size and checkpoint counts). You can override the thresholds used to trigger metadata-bloat recommendations by passing `metadata_file_count_threshold` and/or `metadata_total_size_threshold` to `analyze_delta_lake` (e.g., `analyze_delta_lake(path, metadata_file_count_threshold=200)`).

## Development

### Building

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Install in editable mode for development
pip install -e .

# Build wheel (PEP 517)
python -m build
```

### Testing

Delta-Skelter includes a comprehensive Python test suite, with automated CI/CD testing across multiple platforms and supported Python versions.

#### Quick Start

```bash
# Install dependencies and run all tests
make install build test

# Or use the test runner script
python run_tests.py --all
```

#### Test Categories

**Python Tests**
```bash
# Run Python tests
make test-python
# or
python -m pytest tests/ -v

# Run with coverage
make coverage
# or
python -m pytest tests/ --cov=delta_skelter --cov-report=html
```

**Integration Tests**
```bash
# Run integration tests
make test-integration
# or
python -m pytest tests/ -m integration -v
```

#### Test Infrastructure

**Test Structure**
```
tests/
├── __init__.py              # Test package initialization
├── conftest.py              # Pytest configuration and fixtures
├── test_delta_skelter.py         # Main test suite for delta_skelter module
└── README.md               # Detailed testing documentation
```

**Test Markers**
```bash
# Run specific test categories
python -m pytest tests/ -m unit -v          # Unit tests only
python -m pytest tests/ -m integration -v   # Integration tests only
python -m pytest tests/ -m mock -v          # Mock tests only
python -m pytest tests/ -m real -v          # Real service tests only
```

#### Code Quality

**Linting and Formatting**
```bash
# Run all linting checks
make lint

# Format code
make format



# Check Python formatting
black --check tests/ examples/

# Check Python linting
flake8 tests/ examples/ --max-line-length=100
```

**Security Checks**
```bash
# Run security audits
make security

 
# Python security check
safety check

# Python security linting
bandit -r tests/ examples/
```

#### Performance Testing

```bash
# Run performance benchmarks
make perf
# or
python -m pytest tests/ -v --benchmark-only --benchmark-sort=mean
```

#### Development Workflow

**Full CI Pipeline**
```bash
# Run complete CI pipeline locally
make ci

# Quick development test
make quick-test

# Pre-commit checks
make pre-commit
```

**Git Hooks Setup**
```bash
# Setup pre-commit hooks
make setup-hooks

# Remove hooks
make remove-hooks
```

#### Continuous Integration

Delta-Skelter uses GitHub Actions for automated testing on:

- **Operating Systems**: Ubuntu, Windows, macOS
- **Python Versions**: 3.8, 3.9, 3.10, 3.11, 3.12
 

**CI Pipeline Includes:**
1. **Multi-Platform Testing**: Tests run on all supported platforms
2. **Security Scanning**: Automated vulnerability detection
3. **Performance Benchmarks**: Performance regression detection
4. **Documentation Generation**: Automatic doc validation
5. **Code Coverage**: Coverage reporting with Codecov integration
6. **Artifact Building**: Wheel building for all platforms
7. **Release Automation**: Automatic PyPI publishing

#### Test Coverage

The test suite provides comprehensive coverage:

- **Unit Tests**: Individual function and method testing
- **Integration Tests**: End-to-end workflow testing
- **Mock Tests**: Testing with mocked dependencies
- **Edge Cases**: Boundary conditions and error scenarios
- **Performance Tests**: Benchmark and performance regression testing

#### Writing Tests

**Test Naming Convention**
- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`

**Example Test**
```python
def test_analyze_delta_lake_parameters():
    """Test analyze_delta_lake function parameters."""
    with patch('delta_skelter.analyze_delta_lake') as mock_analyze:
        mock_report = MagicMock()
        mock_analyze.return_value = mock_report
        
        result = delta_skelter.analyze_delta_lake(
          "abfss://testfs@account.dfs.core.windows.net/test-table/",
        )

        mock_analyze.assert_called_once_with(
          "abfss://testfs@account.dfs.core.windows.net/test-table/",
        )
        assert result == mock_report
```

**Using Fixtures**
```python
def test_with_mock_report(mock_health_report):
    """Test with mock health report."""
    assert mock_health_report.table_path == "abfss://testfs@account.dfs.core.windows.net/test-table/"
    assert mock_health_report.health_score == 0.85
```

#### Debugging Tests

```bash
# Run specific test file
python -m pytest tests/test_delta_skelter.py -v

# Run specific test function
python -m pytest tests/test_delta_skelter.py::TestDelta-SkelterModule::test_analyze_delta_lake_parameters -v

# Run tests matching pattern
python -m pytest tests/ -k "delta_lake" -v

# Debug mode
python -m pytest tests/ -v -s --pdb
```

#### Available Make Targets

```bash
make help                    # Show all available targets
make install                 # Install dependencies
make build                   # Build the project
make test                    # Run all tests
make test-python            # Run Python tests only
make test-integration       # Run integration tests only
make lint                    # Run linting checks
make format                  # Format code
make security               # Run security checks
make coverage               # Run with coverage
make clean                  # Clean build artifacts
make release                # Build release version
make docs                   # Generate documentation
make ci                     # Run full CI pipeline
make dev                    # Development setup
make quick-test             # Quick test (unit tests only)
make examples               # Test examples
make check                  # Run all checks
make pre-commit             # Pre-commit checks
make post-commit            # Post-commit checks
make setup-hooks            # Setup git hooks
make remove-hooks           # Remove git hooks
make info                   # Show project info
```

#### Troubleshooting

**Common Issues**
1. **Import Errors**: Ensure delta_skelter module is built (`make build`)
2. **Missing Dependencies**: Install all requirements (`make install`)
3. **Permission Errors**: Check file permissions
4. **Timeout Errors**: Increase timeout for slow tests

**Getting Help**
- Check test output for error messages
- Use `-v` flag for verbose output
- Use `--pdb` for debugging
- Check CI logs for detailed error information
- See `tests/README.md` for detailed testing documentation

## Performance

Delta-Skelter is designed for speed:

- ⚡ Async I/O for concurrent ADLS operations
- 🐍 Python-first implementation optimized for clarity and portability
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

## Releasing

To release a new version:

1. Update the version in `pyproject.toml`
2. Commit and push changes
3. Create and push a version tag:
   ```bash
   git tag -a v0.1.8 -m "Release v0.1.8"
   git push origin main
   git push origin v0.1.8
   ```
4. GitHub Actions will automatically build and publish to PyPI

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details
