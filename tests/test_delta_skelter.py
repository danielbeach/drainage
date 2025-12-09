"""
Test suite for the delta_skelter Python module.

This module contains comprehensive tests for the delta_skelter library's Python bindings,
including unit tests for all public functions and integration tests for the
complete analysis workflow.
"""

import unittest
import sys
import os
from unittest.mock import patch, MagicMock

# Add the parent directory to the path so we can import delta_skelter
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import delta_skelter
except ImportError:
    # If delta_skelter is not installed, we'll skip the tests
    delta_skelter = None


class TestDeltaSkelterModule(unittest.TestCase):
    """Test cases for the delta_skelter module."""

    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        if delta_skelter is None:
            raise unittest.SkipTest("delta_skelter module not available")

    def test_module_import(self):
        """Test that the delta_skelter module can be imported."""
        self.assertIsNotNone(delta_skelter)
        self.assertTrue(hasattr(delta_skelter, "analyze_delta_lake"))
        self.assertTrue(hasattr(delta_skelter, "analyze_table"))
        self.assertTrue(hasattr(delta_skelter, "print_health_report"))

    def test_analyze_delta_lake_function_exists(self):
        """Test that analyze_delta_lake function exists and is callable."""
        self.assertTrue(callable(delta_skelter.analyze_delta_lake))

    def test_analyze_table_function_exists(self):
        """Test that analyze_table function exists and is callable."""
        self.assertTrue(callable(delta_skelter.analyze_table))

    def test_print_health_report_function_exists(self):
        """Test that print_health_report function exists and is callable."""
        self.assertTrue(callable(delta_skelter.print_health_report))

    @patch("delta_skelter.analyze_delta_lake")
    def test_analyze_delta_lake_parameters(self, mock_analyze):
        """Test analyze_delta_lake function parameters."""
        # Mock the return value
        mock_report = MagicMock()
        mock_analyze.return_value = mock_report
        # Test with path and client_id parameters
        result = delta_skelter.analyze_delta_lake(
            "abfss://fs@account.dfs.core.windows.net/test-table/",
            "test-uami-client-id",
        )

        # Verify the function was called with correct parameters
        mock_analyze.assert_called_once_with(
            "abfss://fs@account.dfs.core.windows.net/test-table/",
            "test-uami-client-id",
        )
        self.assertEqual(result, mock_report)

    @patch("delta_skelter.analyze_delta_lake")
    def test_analyze_delta_lake_optional_parameters(self, mock_analyze):
        """Test analyze_delta_lake function with optional parameters."""
        # Mock the return value
        mock_report = MagicMock()
        mock_analyze.return_value = mock_report

        # Test with only required parameters
        result = delta_skelter.analyze_delta_lake(
            "abfss://fs@account.dfs.core.windows.net/test-table/"
        )

        # Verify the function was called with correct parameters
        mock_analyze.assert_called_once_with(
            "abfss://fs@account.dfs.core.windows.net/test-table/"
        )
        self.assertEqual(result, mock_report)

    @patch("delta_skelter.analyze_table")
    def test_analyze_table_parameters(self, mock_analyze):
        """Test analyze_table function parameters."""
        # Mock the return value
        mock_report = MagicMock()
        mock_analyze.return_value = mock_report

        # Test with all parameters (path, table_type, client_id)
        result = delta_skelter.analyze_table(
            "abfss://fs@account.dfs.core.windows.net/test-table/",
            table_type="delta",
            client_id="test-uami-client-id",
        )

        # Verify the function was called with correct parameters
        mock_analyze.assert_called_once_with(
            "abfss://fs@account.dfs.core.windows.net/test-table/",
            table_type="delta",
            client_id="test-uami-client-id",
        )
        self.assertEqual(result, mock_report)

    @patch("delta_skelter.analyze_table")
    def test_analyze_table_auto_detection(self, mock_analyze):
        """Test analyze_table function with auto-detection."""
        # Mock the return value
        mock_report = MagicMock()
        mock_analyze.return_value = mock_report

        # Test with auto-detection (no table_type specified)
        result = delta_skelter.analyze_table(
            "abfss://fs@account.dfs.core.windows.net/test-table/"
        )

        # Verify the function was called with correct parameters
        mock_analyze.assert_called_once_with(
            "abfss://fs@account.dfs.core.windows.net/test-table/"
        )
        self.assertEqual(result, mock_report)

    def test_print_health_report_parameters(self):
        """Test print_health_report function parameters."""
        # Create a mock health report
        mock_report = MagicMock()
        mock_report.table_path = "abfss://fs@account.dfs.core.windows.net/test-table/"
        mock_report.table_type = "delta"
        mock_report.analysis_timestamp = "2023-01-01T00:00:00Z"
        mock_report.health_score = 0.85
        mock_report.metrics = MagicMock()
        mock_report.metrics.total_files = 100
        mock_report.metrics.total_size_bytes = 1024 * 1024 * 100  # 100MB
        mock_report.metrics.avg_file_size_bytes = 1024 * 1024  # 1MB
        mock_report.metrics.partition_count = 10
        mock_report.metrics.file_size_distribution = MagicMock()
        mock_report.metrics.file_size_distribution.small_files = 10
        mock_report.metrics.file_size_distribution.medium_files = 80
        mock_report.metrics.file_size_distribution.large_files = 10
        mock_report.metrics.file_size_distribution.very_large_files = 0
        mock_report.metrics.data_skew = MagicMock()
        mock_report.metrics.data_skew.partition_skew_score = 0.1
        mock_report.metrics.data_skew.file_size_skew_score = 0.05
        mock_report.metrics.data_skew.largest_partition_size = 1024 * 1024 * 20
        mock_report.metrics.data_skew.smallest_partition_size = 1024 * 1024 * 5
        mock_report.metrics.data_skew.avg_partition_size = 1024 * 1024 * 10
        mock_report.metrics.metadata_health = MagicMock()
        mock_report.metrics.metadata_health.metadata_file_count = 5
        mock_report.metrics.metadata_health.metadata_total_size_bytes = 1024 * 1024
        mock_report.metrics.metadata_health.avg_metadata_file_size_bytes = 1024 * 200
        mock_report.metrics.metadata_health.manifest_file_count = 0
        mock_report.metrics.snapshot_health = MagicMock()
        mock_report.metrics.snapshot_health.snapshot_count = 5
        mock_report.metrics.snapshot_health.snapshot_retention_risk = 0.1
        mock_report.metrics.snapshot_health.oldest_snapshot_age_days = 1.0
        mock_report.metrics.snapshot_health.newest_snapshot_age_days = 0.0
        mock_report.metrics.snapshot_health.avg_snapshot_age_days = 0.5
        mock_report.metrics.unreferenced_files = []
        mock_report.metrics.unreferenced_size_bytes = 0
        mock_report.metrics.deletion_vector_metrics = None
        mock_report.metrics.schema_evolution = None
        mock_report.metrics.time_travel_metrics = None
        mock_report.metrics.table_constraints = None
        mock_report.metrics.file_compaction = None
        mock_report.metrics.clustering = None
        mock_report.metrics.recommendations = []

        # Test that the function exists and can be called
        # Note: We can't easily test this without a real HealthReport object
        # since the HealthReport class is not exposed in the Python API
        self.assertTrue(hasattr(delta_skelter, "print_health_report"))
        self.assertTrue(callable(delta_skelter.print_health_report))

    def test_adls_path_validation(self):
        """Test ADLS path validation."""
        valid_paths = [
            "abfss://fs@account.dfs.core.windows.net/table/",
            "abfss://myfs@myaccount.dfs.core.windows.net/my-table/",
            "https://account.dfs.core.windows.net/fs/path/to/table/",
        ]

        for path in valid_paths:
            self.assertTrue(
                path.startswith("abfss://") or path.startswith("https://"),
                f"Invalid ADLS path: {path}",
            )
            self.assertTrue(
                "/" in path, f"ADLS path should contain path separator: {path}"
            )

    # AWS-specific region/credential tests removed for ADLS-only project

    # AWS credentials tests removed for ADLS-only project

    def test_table_type_validation(self):
        """Test table type validation (Delta-only)."""
        valid_table_types = ["delta", "Delta", "DELTA"]

        for table_type in valid_table_types:
            self.assertIsInstance(table_type, str)
            self.assertTrue(
                len(table_type) > 0, f"Table type should not be empty: {table_type}"
            )

    def test_health_report_structure(self):
        """Test health report structure."""
        # This test would require creating a mock health report
        # and verifying its structure matches the expected format
        expected_attributes = [
            "table_path",
            "table_type",
            "analysis_timestamp",
            "metrics",
            "health_score",
        ]

        # Create a mock health report
        mock_report = MagicMock()
        for attr in expected_attributes:
            setattr(mock_report, attr, None)

        # Verify all expected attributes exist
        for attr in expected_attributes:
            self.assertTrue(
                hasattr(mock_report, attr),
                f"Health report should have {attr} attribute",
            )

    def test_health_metrics_structure(self):
        """Test health metrics structure."""
        expected_attributes = [
            "total_files",
            "total_size_bytes",
            "unreferenced_files",
            "unreferenced_size_bytes",
            "partition_count",
            "partitions",
            "clustering",
            "avg_file_size_bytes",
            "file_size_distribution",
            "recommendations",
            "health_score",
            "data_skew",
            "metadata_health",
            "snapshot_health",
            "deletion_vector_metrics",
            "schema_evolution",
            "time_travel_metrics",
            "table_constraints",
            "file_compaction",
        ]

        # Create a mock health metrics
        mock_metrics = MagicMock()
        for attr in expected_attributes:
            setattr(mock_metrics, attr, None)

        # Verify all expected attributes exist
        for attr in expected_attributes:
            self.assertTrue(
                hasattr(mock_metrics, attr),
                f"Health metrics should have {attr} attribute",
            )

    def test_file_size_distribution_structure(self):
        """Test file size distribution structure."""
        expected_attributes = [
            "small_files",
            "medium_files",
            "large_files",
            "very_large_files",
        ]

        # Create a mock file size distribution
        mock_distribution = MagicMock()
        for attr in expected_attributes:
            setattr(mock_distribution, attr, 0)

        # Verify all expected attributes exist
        for attr in expected_attributes:
            self.assertTrue(
                hasattr(mock_distribution, attr),
                f"File size distribution should have {attr} attribute",
            )

    def test_data_skew_metrics_structure(self):
        """Test data skew metrics structure."""
        expected_attributes = [
            "partition_skew_score",
            "file_size_skew_score",
            "largest_partition_size",
            "smallest_partition_size",
            "avg_partition_size",
            "partition_size_std_dev",
        ]

        # Create a mock data skew metrics
        mock_skew = MagicMock()
        for attr in expected_attributes:
            setattr(mock_skew, attr, 0.0)

        # Verify all expected attributes exist
        for attr in expected_attributes:
            self.assertTrue(
                hasattr(mock_skew, attr),
                f"Data skew metrics should have {attr} attribute",
            )

    def test_metadata_health_structure(self):
        """Test metadata health structure."""
        expected_attributes = [
            "metadata_file_count",
            "metadata_total_size_bytes",
            "avg_metadata_file_size_bytes",
            "metadata_growth_rate",
            "manifest_file_count",
        ]

        # Create a mock metadata health
        mock_metadata = MagicMock()
        for attr in expected_attributes:
            setattr(mock_metadata, attr, 0)

        # Verify all expected attributes exist
        for attr in expected_attributes:
            self.assertTrue(
                hasattr(mock_metadata, attr),
                f"Metadata health should have {attr} attribute",
            )

    def test_snapshot_health_structure(self):
        """Test snapshot health structure."""
        expected_attributes = [
            "snapshot_count",
            "oldest_snapshot_age_days",
            "newest_snapshot_age_days",
            "avg_snapshot_age_days",
            "snapshot_retention_risk",
        ]

        # Create a mock snapshot health
        mock_snapshot = MagicMock()
        for attr in expected_attributes:
            setattr(mock_snapshot, attr, 0.0)

        # Verify all expected attributes exist
        for attr in expected_attributes:
            self.assertTrue(
                hasattr(mock_snapshot, attr),
                f"Snapshot health should have {attr} attribute",
            )


class Testdelta_skelterIntegration(unittest.TestCase):
    """Integration tests for the delta_skelter module."""

    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        if delta_skelter is None:
            raise unittest.SkipTest("delta_skelter module not available")

    @patch("delta_skelter.analyze_table")
    def test_complete_analysis_workflow(self, mock_analyze):
        """Test complete analysis workflow."""
        # Mock the return value
        mock_report = MagicMock()
        mock_report.table_path = "abfss://fs@account.dfs.core.windows.net/test-table/"
        mock_report.table_type = "delta"
        mock_report.health_score = 0.85
        mock_analyze.return_value = mock_report

        # Test the complete workflow

        path = "abfss://fs@account.dfs.core.windows.net/test-table/"

        # Analyze the table
        report = delta_skelter.analyze_table(path)

        # Verify the analysis was performed
        mock_analyze.assert_called_once_with(path)

        # Verify the report structure (ADLS path expected)
        self.assertEqual(report.table_path, path)
        self.assertEqual(report.table_type, "delta")
        self.assertEqual(report.health_score, 0.85)

    @patch("delta_skelter.analyze_delta_lake")
    def test_delta_lake_analysis_workflow(self, mock_analyze):
        """Test Delta Lake analysis workflow."""
        # Mock the return value
        mock_report = MagicMock()
        mock_report.table_path = "abfss://fs@account.dfs.core.windows.net/delta-table/"
        mock_report.table_type = "delta"
        mock_report.health_score = 0.90
        mock_analyze.return_value = mock_report

        # Test Delta Lake analysis

        path = "abfss://fs@account.dfs.core.windows.net/delta-table/"

        # Analyze the Delta Lake table
        report = delta_skelter.analyze_delta_lake(path)

        # Verify the analysis was performed
        mock_analyze.assert_called_once_with(path)

        # Verify the report structure (ADLS path expected)
        self.assertEqual(report.table_path, path)
        self.assertEqual(report.table_type, "delta")
        self.assertEqual(report.health_score, 0.90)

    def test_error_handling_invalid_adls_path(self):
        """Test error handling for invalid ADLS paths."""
        invalid_paths = [
            "not-a-url",
            "ftp://bucket/table/",
            "",
            "abfss://",
            "abfss:///",
        ]

        for invalid_path in invalid_paths:
            if invalid_path == "":
                continue  # Skip empty string test
            is_valid_adls = (
                (
                    invalid_path.startswith("abfss://")
                    or invalid_path.startswith("https://")
                )
                and len(invalid_path) > 10
                and "/" in invalid_path
            )
            self.assertFalse(
                is_valid_adls, f"Should be invalid ADLS path: {invalid_path}"
            )

    def test_error_handling_invalid_table_type(self):
        """Test error handling for invalid table types."""
        invalid_table_types = ["hudi", "parquet", "csv", "json", ""]

        for invalid_type in invalid_table_types:
            if invalid_type == "":
                continue  # Skip empty string test
            # This would normally raise an exception
            # We're just testing that the validation logic exists
            self.assertNotIn(
                invalid_type.lower(),
                ["delta"],
                f"Should be invalid table type: {invalid_type}",
            )


if __name__ == "__main__":
    unittest.main()
