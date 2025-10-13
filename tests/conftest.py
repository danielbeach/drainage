"""
Pytest configuration and fixtures for drainage tests.

This module provides common fixtures and configuration for testing the drainage library.
"""

import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Add the parent directory to the path so we can import drainage
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import drainage
except ImportError:
    drainage = None


@pytest.fixture(scope="session")
def drainage_module():
    """Provide the drainage module for testing."""
    if drainage is None:
        pytest.skip("drainage module not available")
    return drainage


@pytest.fixture
def mock_health_report():
    """Create a mock health report for testing."""
    mock_report = MagicMock()
    mock_report.table_path = "s3://test-bucket/test-table/"
    mock_report.table_type = "delta"
    mock_report.analysis_timestamp = "2023-01-01T00:00:00Z"
    mock_report.health_score = 0.85

    # Mock metrics
    mock_report.metrics = MagicMock()
    mock_report.metrics.total_files = 100
    mock_report.metrics.total_size_bytes = 1024 * 1024 * 100  # 100MB
    mock_report.metrics.avg_file_size_bytes = 1024 * 1024  # 1MB
    mock_report.metrics.partition_count = 10
    mock_report.metrics.unreferenced_files = []
    mock_report.metrics.unreferenced_size_bytes = 0
    mock_report.metrics.partitions = []
    mock_report.metrics.clustering = None
    mock_report.metrics.recommendations = []

    # Mock file size distribution
    mock_report.metrics.file_size_distribution = MagicMock()
    mock_report.metrics.file_size_distribution.small_files = 10
    mock_report.metrics.file_size_distribution.medium_files = 80
    mock_report.metrics.file_size_distribution.large_files = 10
    mock_report.metrics.file_size_distribution.very_large_files = 0

    # Mock data skew metrics
    mock_report.metrics.data_skew = MagicMock()
    mock_report.metrics.data_skew.partition_skew_score = 0.1
    mock_report.metrics.data_skew.file_size_skew_score = 0.05
    mock_report.metrics.data_skew.largest_partition_size = 1024 * 1024 * 20
    mock_report.metrics.data_skew.smallest_partition_size = 1024 * 1024 * 5
    mock_report.metrics.data_skew.avg_partition_size = 1024 * 1024 * 10
    mock_report.metrics.data_skew.partition_size_std_dev = 1024 * 1024 * 2

    # Mock metadata health
    mock_report.metrics.metadata_health = MagicMock()
    mock_report.metrics.metadata_health.metadata_file_count = 5
    mock_report.metrics.metadata_health.metadata_total_size_bytes = 1024 * 1024
    mock_report.metrics.metadata_health.avg_metadata_file_size = 1024 * 200
    mock_report.metrics.metadata_health.metadata_growth_rate = 0.0
    mock_report.metrics.metadata_health.manifest_file_count = 0

    # Mock snapshot health
    mock_report.metrics.snapshot_health = MagicMock()
    mock_report.metrics.snapshot_health.snapshot_count = 5
    mock_report.metrics.snapshot_health.snapshot_retention_risk = 0.1
    mock_report.metrics.snapshot_health.oldest_snapshot_age_days = 1.0
    mock_report.metrics.snapshot_health.newest_snapshot_age_days = 0.0
    mock_report.metrics.snapshot_health.avg_snapshot_age_days = 0.5

    # Mock optional metrics
    mock_report.metrics.deletion_vector_metrics = None
    mock_report.metrics.schema_evolution = None
    mock_report.metrics.time_travel_metrics = None
    mock_report.metrics.table_constraints = None
    mock_report.metrics.file_compaction = None

    return mock_report


@pytest.fixture
def mock_delta_lake_objects():
    """Create mock Delta Lake objects for testing."""
    return [
        MagicMock(
            key="part-00000.parquet",
            size=1024 * 1024,
            last_modified="2023-01-01T00:00:00Z",
            etag="etag1",
        ),
        MagicMock(
            key="part-00001.parquet",
            size=1024 * 1024,
            last_modified="2023-01-01T00:00:00Z",
            etag="etag2",
        ),
        MagicMock(
            key="_delta_log/00000000000000000000.json",
            size=2048,
            last_modified="2023-01-01T00:00:00Z",
            etag="etag3",
        ),
        MagicMock(
            key="_delta_log/00000000000000000001.json",
            size=1024,
            last_modified="2023-01-01T00:00:00Z",
            etag="etag4",
        ),
    ]


@pytest.fixture
def mock_iceberg_objects():
    """Create mock Iceberg objects for testing."""
    return [
        MagicMock(
            key="data/00000-0-00000000000000000000-00000000000000000000.parquet",
            size=1024 * 1024,
            last_modified="2023-01-01T00:00:00Z",
            etag="etag1",
        ),
        MagicMock(
            key="data/00000-1-00000000000000000000-00000000000000000000.parquet",
            size=1024 * 1024,
            last_modified="2023-01-01T00:00:00Z",
            etag="etag2",
        ),
        MagicMock(
            key="metadata/00000-00000000000000000000.metadata.json",
            size=2048,
            last_modified="2023-01-01T00:00:00Z",
            etag="etag3",
        ),
        MagicMock(
            key="metadata/snap-00000000000000000000-1-00000000000000000000.avro",
            size=1024,
            last_modified="2023-01-01T00:00:00Z",
            etag="etag4",
        ),
    ]


@pytest.fixture
def valid_s3_paths():
    """Provide valid S3 paths for testing."""
    return [
        "s3://bucket/table/",
        "s3://my-bucket/my-table/",
        "s3://bucket.with.dots/table/",
        "s3://bucket/path/to/table/",
    ]


@pytest.fixture
def invalid_s3_paths():
    """Provide invalid S3 paths for testing."""
    return [
        "not-a-url",
        "https://bucket/table/",
        "ftp://bucket/table/",
        "s3://",
        "s3:///",
    ]


@pytest.fixture
def valid_aws_regions():
    """Provide valid AWS regions for testing."""
    return [
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "ap-southeast-1",
        "ca-central-1",
    ]


@pytest.fixture
def valid_aws_credentials():
    """Provide valid AWS credentials for testing."""
    return {
        "access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    }


@pytest.fixture
def valid_table_types():
    """Provide valid table types for testing."""
    return ["delta", "iceberg", "Delta", "Iceberg", "DELTA", "ICEBERG"]


@pytest.fixture
def invalid_table_types():
    """Provide invalid table types for testing."""
    return ["hudi", "parquet", "csv", "json", ""]


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client for testing."""
    mock_client = MagicMock()
    mock_client.list_objects_v2.return_value = MagicMock()
    mock_client.get_object.return_value = MagicMock()
    return mock_client


@pytest.fixture
def mock_aws_config():
    """Create a mock AWS config for testing."""
    mock_config = MagicMock()
    mock_config.region.return_value = "us-west-2"
    return mock_config


@pytest.fixture
def mock_aws_credentials():
    """Create mock AWS credentials for testing."""
    mock_creds = MagicMock()
    mock_creds.access_key_id.return_value = "AKIAIOSFODNN7EXAMPLE"
    mock_creds.secret_access_key.return_value = (
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    )
    mock_creds.session_token.return_value = None
    mock_creds.expiry.return_value = None
    mock_creds.provider_name.return_value = "drainage"
    return mock_creds


@pytest.fixture(autouse=True)
def mock_aws_environment():
    """Mock AWS environment variables for testing."""
    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "test-access-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret-key",
            "AWS_DEFAULT_REGION": "us-west-2",
        },
    ):
        yield


@pytest.fixture
def mock_tokio_runtime():
    """Mock the tokio runtime for testing."""
    with patch("drainage.tokio.runtime.Runtime") as mock_runtime:
        mock_rt = MagicMock()
        mock_rt.block_on.return_value = MagicMock()
        mock_runtime.new.return_value = mock_rt
        yield mock_rt


@pytest.fixture
def mock_health_analyzer():
    """Create a mock health analyzer for testing."""
    mock_analyzer = MagicMock()
    mock_analyzer.get_table_info.return_value = ("test-bucket", "test-prefix")
    return mock_analyzer


@pytest.fixture
def mock_delta_lake_analyzer():
    """Create a mock Delta Lake analyzer for testing."""
    mock_analyzer = MagicMock()
    mock_analyzer.analyze.return_value = MagicMock()
    return mock_analyzer


@pytest.fixture
def mock_iceberg_analyzer():
    """Create a mock Iceberg analyzer for testing."""
    mock_analyzer = MagicMock()
    mock_analyzer.analyze.return_value = MagicMock()
    return mock_analyzer


@pytest.fixture
def mock_s3_client_wrapper():
    """Create a mock S3 client wrapper for testing."""
    mock_wrapper = MagicMock()
    mock_wrapper.get_bucket.return_value = "test-bucket"
    mock_wrapper.get_prefix.return_value = "test-prefix"
    mock_wrapper.list_objects.return_value = []
    mock_wrapper.get_object.return_value = b"test data"
    return mock_wrapper


@pytest.fixture
def mock_health_metrics():
    """Create mock health metrics for testing."""
    mock_metrics = MagicMock()
    mock_metrics.total_files = 100
    mock_metrics.total_size_bytes = 1024 * 1024 * 100
    mock_metrics.unreferenced_files = []
    mock_metrics.unreferenced_size_bytes = 0
    mock_metrics.partition_count = 10
    mock_metrics.partitions = []
    mock_metrics.clustering = None
    mock_metrics.avg_file_size_bytes = 1024 * 1024
    mock_metrics.file_size_distribution = MagicMock()
    mock_metrics.file_size_distribution.small_files = 10
    mock_metrics.file_size_distribution.medium_files = 80
    mock_metrics.file_size_distribution.large_files = 10
    mock_metrics.file_size_distribution.very_large_files = 0
    mock_metrics.recommendations = []
    mock_metrics.health_score = 0.85
    mock_metrics.data_skew = MagicMock()
    mock_metrics.metadata_health = MagicMock()
    mock_metrics.snapshot_health = MagicMock()
    mock_metrics.deletion_vector_metrics = None
    mock_metrics.schema_evolution = None
    mock_metrics.time_travel_metrics = None
    mock_metrics.table_constraints = None
    mock_metrics.file_compaction = None
    return mock_metrics


@pytest.fixture
def mock_file_info():
    """Create mock file info for testing."""
    mock_file = MagicMock()
    mock_file.path = "test/file.parquet"
    mock_file.size_bytes = 1024 * 1024
    mock_file.last_modified = "2023-01-01T00:00:00Z"
    mock_file.is_referenced = True
    return mock_file


@pytest.fixture
def mock_partition_info():
    """Create mock partition info for testing."""
    mock_partition = MagicMock()
    mock_partition.partition_values = {"year": "2023", "month": "01"}
    mock_partition.file_count = 10
    mock_partition.total_size_bytes = 1024 * 1024 * 10
    mock_partition.avg_file_size_bytes = 1024 * 1024
    mock_partition.files = []
    return mock_partition


@pytest.fixture
def mock_clustering_info():
    """Create mock clustering info for testing."""
    mock_clustering = MagicMock()
    mock_clustering.clustering_columns = ["col1", "col2"]
    mock_clustering.cluster_count = 5
    mock_clustering.avg_files_per_cluster = 20.0
    mock_clustering.avg_cluster_size_bytes = 2000.0
    return mock_clustering


@pytest.fixture
def mock_file_size_distribution():
    """Create mock file size distribution for testing."""
    mock_distribution = MagicMock()
    mock_distribution.small_files = 10
    mock_distribution.medium_files = 80
    mock_distribution.large_files = 10
    mock_distribution.very_large_files = 0
    return mock_distribution


@pytest.fixture
def mock_data_skew_metrics():
    """Create mock data skew metrics for testing."""
    mock_skew = MagicMock()
    mock_skew.partition_skew_score = 0.1
    mock_skew.file_size_skew_score = 0.05
    mock_skew.largest_partition_size = 1024 * 1024 * 20
    mock_skew.smallest_partition_size = 1024 * 1024 * 5
    mock_skew.avg_partition_size = 1024 * 1024 * 10
    mock_skew.partition_size_std_dev = 1024 * 1024 * 2
    return mock_skew


@pytest.fixture
def mock_metadata_health():
    """Create mock metadata health for testing."""
    mock_metadata = MagicMock()
    mock_metadata.metadata_file_count = 5
    mock_metadata.metadata_total_size_bytes = 1024 * 1024
    mock_metadata.avg_metadata_file_size = 1024 * 200
    mock_metadata.metadata_growth_rate = 0.0
    mock_metadata.manifest_file_count = 0
    return mock_metadata


@pytest.fixture
def mock_snapshot_health():
    """Create mock snapshot health for testing."""
    mock_snapshot = MagicMock()
    mock_snapshot.snapshot_count = 5
    mock_snapshot.snapshot_retention_risk = 0.1
    mock_snapshot.oldest_snapshot_age_days = 1.0
    mock_snapshot.newest_snapshot_age_days = 0.0
    mock_snapshot.avg_snapshot_age_days = 0.5
    return mock_snapshot


@pytest.fixture
def mock_deletion_vector_metrics():
    """Create mock deletion vector metrics for testing."""
    mock_dv = MagicMock()
    mock_dv.deletion_vector_count = 5
    mock_dv.total_deletion_vector_size_bytes = 1024 * 1024
    mock_dv.avg_deletion_vector_size_bytes = 1024 * 200
    mock_dv.deletion_vector_age_days = 10.0
    mock_dv.deleted_rows_count = 1000
    mock_dv.deletion_vector_impact_score = 0.5
    return mock_dv


@pytest.fixture
def mock_schema_evolution_metrics():
    """Create mock schema evolution metrics for testing."""
    mock_schema = MagicMock()
    mock_schema.total_schema_changes = 10
    mock_schema.breaking_changes = 2
    mock_schema.non_breaking_changes = 8
    mock_schema.schema_stability_score = 0.8
    mock_schema.days_since_last_change = 5.0
    mock_schema.schema_change_frequency = 0.1
    mock_schema.current_schema_version = 10
    return mock_schema


@pytest.fixture
def mock_time_travel_metrics():
    """Create mock time travel metrics for testing."""
    mock_tt = MagicMock()
    mock_tt.total_snapshots = 50
    mock_tt.oldest_snapshot_age_days = 30.0
    mock_tt.newest_snapshot_age_days = 0.0
    mock_tt.total_historical_size_bytes = 5 * 1024 * 1024 * 1024
    mock_tt.avg_snapshot_size_bytes = 100.0 * 1024 * 1024
    mock_tt.storage_cost_impact_score = 0.3
    mock_tt.retention_efficiency_score = 0.7
    mock_tt.recommended_retention_days = 14
    return mock_tt


@pytest.fixture
def mock_table_constraints_metrics():
    """Create mock table constraints metrics for testing."""
    mock_constraints = MagicMock()
    mock_constraints.total_constraints = 8
    mock_constraints.check_constraints = 3
    mock_constraints.not_null_constraints = 4
    mock_constraints.unique_constraints = 1
    mock_constraints.foreign_key_constraints = 0
    mock_constraints.constraint_violation_risk = 0.2
    mock_constraints.data_quality_score = 0.9
    mock_constraints.constraint_coverage_score = 0.8
    return mock_constraints


@pytest.fixture
def mock_file_compaction_metrics():
    """Create mock file compaction metrics for testing."""
    mock_compaction = MagicMock()
    mock_compaction.compaction_opportunity_score = 0.7
    mock_compaction.small_files_count = 25
    mock_compaction.small_files_size_bytes = 50 * 1024 * 1024
    mock_compaction.potential_compaction_files = 25
    mock_compaction.estimated_compaction_savings_bytes = 10 * 1024 * 1024
    mock_compaction.recommended_target_file_size_bytes = 128 * 1024 * 1024
    mock_compaction.compaction_priority = "medium"
    mock_compaction.z_order_opportunity = True
    mock_compaction.z_order_columns = ["col1", "col2"]
    return mock_compaction
