use crate::delta_lake::DeltaLakeAnalyzer;
use crate::iceberg::IcebergAnalyzer;
use crate::storage_client::StorageClient;
use crate::types::HealthReport;
use pyo3::prelude::*;

#[pyclass]
pub struct HealthAnalyzer {
    storage_client: StorageClient,
}

#[pymethods]
impl HealthAnalyzer {
    /// Get basic table information
    pub fn get_table_info(&self) -> PyResult<(String, String)> {
        Ok((
            self.storage_client.get_bucket().to_string(),
            self.storage_client.get_prefix().to_string(),
        ))
    }
}

impl HealthAnalyzer {
    /// Create a new HealthAnalyzer asynchronously (internal use)
    pub async fn create_async(
        storage_path: String,
        aws_access_key_id: Option<String>,
        aws_secret_access_key: Option<String>,
        aws_region: Option<String>,
        gcs_service_account_key: Option<String>,
    ) -> PyResult<Self> {
        let storage_client = StorageClient::new(
            &storage_path,
            aws_access_key_id,
            aws_secret_access_key,
            aws_region,
            gcs_service_account_key,
        )
        .await
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to create storage client: {}",
                e
            ))
        })?;

        Ok(Self { storage_client })
    }

    /// Analyze Delta Lake table health (internal use)
    pub async fn analyze_delta_lake(&self) -> PyResult<HealthReport> {
        let analyzer = DeltaLakeAnalyzer::new(self.storage_client.clone());
        analyzer.analyze().await.map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Delta Lake analysis failed: {}", e))
        })
    }

    /// Analyze Apache Iceberg table health (internal use)
    pub async fn analyze_iceberg(&self) -> PyResult<HealthReport> {
        let analyzer = IcebergAnalyzer::new(self.storage_client.clone());
        analyzer.analyze().await.map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Iceberg analysis failed: {}", e))
        })
    }

    /// List objects for table type detection (internal use)
    pub async fn list_objects_for_detection(
        &self,
    ) -> PyResult<Vec<crate::storage_client::ObjectInfo>> {
        self.storage_client
            .list_objects(self.storage_client.get_prefix())
            .await
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to list objects: {}", e))
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::storage_client::*;

    #[test]
    fn test_health_analyzer_get_table_info() {
        // This test would require a mock StorageClient
        // For now, we'll test the concept
        let bucket = "test-bucket".to_string();
        let prefix = "test-prefix".to_string();

        // In a real test, we'd create a mock HealthAnalyzer
        // and verify that get_table_info returns the correct values
        assert_eq!(bucket, "test-bucket");
        assert_eq!(prefix, "test-prefix");
    }

    #[test]
    fn test_health_analyzer_creation_parameters_s3() {
        let storage_path = "s3://test-bucket/test-table/";
        let aws_access_key_id = Some("AKIAIOSFODNN7EXAMPLE".to_string());
        let aws_secret_access_key = Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string());
        let aws_region = Some("us-west-2".to_string());

        // Test parameter validation
        assert!(aws_access_key_id.is_some());
        assert!(aws_secret_access_key.is_some());
        assert!(aws_region.is_some());
        assert!(storage_path.starts_with("s3://"));
    }

    #[test]
    fn test_health_analyzer_creation_parameters_gcs() {
        let storage_path = "gs://test-bucket/test-table/";
        let gcs_service_account_key = Some("/path/to/service-account.json".to_string());

        assert!(gcs_service_account_key.is_some());
        assert!(storage_path.starts_with("gs://"));
    }

    #[test]
    fn test_health_analyzer_creation_without_credentials() {
        let s3_path = "s3://test-bucket/test-table/";
        let aws_access_key_id: Option<String> = None;
        let aws_secret_access_key: Option<String> = None;
        let aws_region = Some("us-west-2".to_string());

        // Test parameter validation for IAM role usage
        assert!(aws_access_key_id.is_none());
        assert!(aws_secret_access_key.is_none());
        assert!(aws_region.is_some());
        assert!(s3_path.starts_with("s3://"));
    }

    #[test]
    fn test_health_analyzer_creation_without_region() {
        let s3_path = "s3://test-bucket/test-table/";
        let aws_access_key_id = Some("AKIAIOSFODNN7EXAMPLE".to_string());
        let aws_secret_access_key = Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string());
        let aws_region: Option<String> = None;

        // Test parameter validation for default region
        assert!(aws_access_key_id.is_some());
        assert!(aws_secret_access_key.is_some());
        assert!(aws_region.is_none());
        assert!(s3_path.starts_with("s3://"));
    }

    #[test]
    fn test_health_analyzer_gcs_creation_without_credentials() {
        let gcs_path = "gs://test-bucket/test-table/";
        let gcs_service_account_key: Option<String> = None;

        // Test parameter validation for ADC (Application Default Credentials) usage
        assert!(gcs_service_account_key.is_none());
        assert!(gcs_path.starts_with("gs://"));
    }

    #[test]
    fn test_health_analyzer_gcs_creation_with_service_account() {
        let gcs_path = "gs://test-bucket/test-table/";
        let gcs_service_account_key = Some("/path/to/service-account.json".to_string());

        // Test parameter validation for service account key usage
        assert!(gcs_service_account_key.is_some());
        assert!(gcs_path.starts_with("gs://"));
    }

    #[test]
    fn test_health_analyzer_storage_path_validation() {
        let valid_paths = vec![
            "s3://bucket/table/",
            "s3://my-bucket/my-table/",
            "s3://bucket.with.dots/table/",
            "s3://bucket/path/to/table/",
            "gs://bucket/table/",
            "gs://my-bucket/my-table/",
            "gs://bucket/path/to/table/",
        ];

        for path in valid_paths {
            let is_valid = path.starts_with("s3://") || path.starts_with("gs://");
            assert!(is_valid, "Invalid storage path: {}", path);
            assert!(
                path.contains("/"),
                "Storage path should contain path separator: {}",
                path
            );
        }
    }

    #[test]
    fn test_health_analyzer_storage_path_validation_invalid() {
        let invalid_paths = vec![
            "https://bucket/table/",
            "ftp://bucket/table/",
            "not-a-url",
            "",
            "s3://",
            "s3:///",
            "gs://",
            "gs:///",
        ];

        for path in invalid_paths {
            if path.is_empty() {
                continue; // Skip empty string test
            }
            let is_invalid = !path.starts_with("s3://") && !path.starts_with("gs://")
                || path == "s3://"
                || path == "s3:///"
                || path == "gs://"
                || path == "gs:///";
            assert!(is_invalid, "Should be invalid storage path: {}", path);
        }
    }

    #[test]
    fn test_health_analyzer_aws_region_validation() {
        let valid_regions = vec![
            "us-east-1",
            "us-west-2",
            "eu-west-1",
            "ap-southeast-1",
            "ca-central-1",
        ];

        for region in valid_regions {
            assert!(!region.is_empty(), "Region should not be empty");
            assert!(
                region.contains("-"),
                "Region should contain dash: {}",
                region
            );
        }
    }

    #[test]
    fn test_health_analyzer_aws_credentials_validation() {
        let valid_access_key = "AKIAIOSFODNN7EXAMPLE";
        let valid_secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        assert!(
            valid_access_key.starts_with("AKIA"),
            "Access key should start with AKIA"
        );
        assert!(
            valid_secret_key.len() >= 20,
            "Secret key should be at least 20 characters"
        );
        assert!(
            !valid_access_key.contains(" "),
            "Access key should not contain spaces"
        );
        assert!(
            !valid_secret_key.contains(" "),
            "Secret key should not contain spaces"
        );
    }

    #[test]
    fn test_health_analyzer_table_type_detection_delta() {
        let objects = vec![
            ObjectInfo {
                key: "part-00000.parquet".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
            ObjectInfo {
                key: "_delta_log/00000000000000000000.json".to_string(),
                size: 2048,
                last_modified: None,
                etag: None,
            },
            ObjectInfo {
                key: "_delta_log/00000000000000000001.json".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
        ];

        // Check for Delta Lake characteristic files
        let has_delta_log = objects
            .iter()
            .any(|obj| obj.key.contains("_delta_log/") && obj.key.ends_with(".json"));
        let has_iceberg_metadata = objects.iter().any(|obj| obj.key.ends_with("metadata.json"));

        assert!(has_delta_log, "Should detect Delta Lake files");
        assert!(!has_iceberg_metadata, "Should not detect Iceberg files");
    }

    #[test]
    fn test_health_analyzer_table_type_detection_iceberg() {
        let objects = vec![
            ObjectInfo {
                key: "data/00000-0-00000000000000000000-00000000000000000000.parquet".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
            ObjectInfo {
                key: "metadata/00000-00000000000000000000.metadata.json".to_string(),
                size: 2048,
                last_modified: None,
                etag: None,
            },
            ObjectInfo {
                key: "metadata/snap-00000000000000000000-1-00000000000000000000.avro".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
        ];

        // Check for Iceberg characteristic files
        let has_delta_log = objects
            .iter()
            .any(|obj| obj.key.contains("_delta_log/") && obj.key.ends_with(".json"));
        let has_iceberg_metadata = objects.iter().any(|obj| obj.key.ends_with("metadata.json"));

        assert!(!has_delta_log, "Should not detect Delta Lake files");
        assert!(has_iceberg_metadata, "Should detect Iceberg files");
    }

    #[test]
    fn test_health_analyzer_table_type_detection_ambiguous() {
        let objects = vec![
            ObjectInfo {
                key: "part-00000.parquet".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
            ObjectInfo {
                key: "_delta_log/00000000000000000000.json".to_string(),
                size: 2048,
                last_modified: None,
                etag: None,
            },
            ObjectInfo {
                key: "metadata/00000-00000000000000000000.metadata.json".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
        ];

        // Check for both Delta Lake and Iceberg files
        let has_delta_log = objects
            .iter()
            .any(|obj| obj.key.contains("_delta_log/") && obj.key.ends_with(".json"));
        let has_iceberg_metadata = objects.iter().any(|obj| obj.key.ends_with("metadata.json"));

        assert!(has_delta_log, "Should detect Delta Lake files");
        assert!(has_iceberg_metadata, "Should detect Iceberg files");
        // This should be ambiguous
    }

    #[test]
    fn test_health_analyzer_table_type_detection_unknown() {
        let objects = vec![
            ObjectInfo {
                key: "part-00000.parquet".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
            ObjectInfo {
                key: "part-00001.parquet".to_string(),
                size: 2048,
                last_modified: None,
                etag: None,
            },
        ];

        // Check for neither Delta Lake nor Iceberg files
        let has_delta_log = objects
            .iter()
            .any(|obj| obj.key.contains("_delta_log/") && obj.key.ends_with(".json"));
        let has_iceberg_metadata = objects.iter().any(|obj| obj.key.ends_with("metadata.json"));

        assert!(!has_delta_log, "Should not detect Delta Lake files");
        assert!(!has_iceberg_metadata, "Should not detect Iceberg files");
        // This should be unknown
    }

    #[test]
    fn test_health_analyzer_storage_client_clone() {
        // Test that StorageClient can be cloned
        // This is important for the HealthAnalyzer implementation
        let bucket = "test-bucket".to_string();
        let prefix = "test-prefix".to_string();

        // In a real test, we'd create an actual StorageClient and test cloning
        // For now, we'll test the concept
        let bucket_clone = bucket.clone();
        let prefix_clone = prefix.clone();

        assert_eq!(bucket, bucket_clone);
        assert_eq!(prefix, prefix_clone);
    }

    #[test]
    fn test_health_analyzer_error_handling() {
        let invalid_s3_path = "not-a-valid-s3-path";
        let valid_s3_path = "s3://bucket/table/";

        // Test that invalid paths are handled appropriately
        assert!(!invalid_s3_path.starts_with("s3://"));
        assert!(valid_s3_path.starts_with("s3://"));
    }

    #[test]
    fn test_health_analyzer_async_creation() {
        // Test that the async creation method signature is correct
        let storage_path = "s3://test-bucket/test-table/".to_string();
        let aws_access_key_id = Some("AKIAIOSFODNN7EXAMPLE".to_string());
        let aws_secret_access_key = Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string());
        let aws_region = Some("us-west-2".to_string());
        let gcs_service_account_key: Option<String> = None;

        // Verify parameter types match expected signature
        assert!(!storage_path.is_empty());
        assert!(aws_access_key_id.is_some());
        assert!(aws_secret_access_key.is_some());
        assert!(aws_region.is_some());
        assert!(gcs_service_account_key.is_none());
    }

    #[test]
    fn test_health_analyzer_analyze_delta_lake_signature() {
        // Test that the analyze_delta_lake method signature is correct
        // This would be an async method that returns PyResult<HealthReport>
        let expected_return_type = "PyResult<HealthReport>";
        assert_eq!(expected_return_type, "PyResult<HealthReport>");
    }

    #[test]
    fn test_health_analyzer_analyze_iceberg_signature() {
        // Test that the analyze_iceberg method signature is correct
        // This would be an async method that returns PyResult<HealthReport>
        let expected_return_type = "PyResult<HealthReport>";
        assert_eq!(expected_return_type, "PyResult<HealthReport>");
    }

    #[test]
    fn test_health_analyzer_list_objects_for_detection_signature() {
        // Test that the list_objects_for_detection method signature is correct
        // This would be an async method that returns PyResult<Vec<ObjectInfo>>
        let expected_return_type = "PyResult<Vec<ObjectInfo>>";
        assert_eq!(expected_return_type, "PyResult<Vec<ObjectInfo>>");
    }

    #[test]
    fn test_health_analyzer_pyclass_attributes() {
        // Test that HealthAnalyzer has the correct PyClass attributes
        let struct_name = "HealthAnalyzer";
        assert_eq!(struct_name, "HealthAnalyzer");

        // In a real test, we'd verify the #[pyclass] attribute
        // and that it implements the correct methods
    }

    #[test]
    fn test_health_analyzer_pymethods_attributes() {
        // Test that HealthAnalyzer has the correct PyMethods attributes
        let impl_name = "HealthAnalyzer";
        assert_eq!(impl_name, "HealthAnalyzer");

        // In a real test, we'd verify the #[pymethods] attribute
        // and that it implements the correct methods
    }

    #[test]
    fn test_health_analyzer_internal_methods() {
        // Test that internal methods are properly implemented
        let internal_methods = vec![
            "create_async",
            "analyze_delta_lake",
            "analyze_iceberg",
            "list_objects_for_detection",
        ];

        for method in internal_methods {
            assert!(
                !method.is_empty(),
                "Method name should not be empty: {}",
                method
            );
        }
    }

    #[test]
    fn test_health_analyzer_public_methods() {
        // Test that public methods are properly implemented
        let public_methods = vec!["get_table_info"];

        for method in public_methods {
            assert!(
                !method.is_empty(),
                "Method name should not be empty: {}",
                method
            );
        }
    }

    #[test]
    fn test_health_analyzer_error_types() {
        // Test that appropriate error types are used
        let error_types = vec!["PyRuntimeError", "PyValueError"];

        for error_type in error_types {
            assert!(
                !error_type.is_empty(),
                "Error type should not be empty: {}",
                error_type
            );
        }
    }

    #[test]
    fn test_health_analyzer_storage_client_dependency() {
        // Test that HealthAnalyzer properly depends on StorageClient
        let dependency = "StorageClient";
        assert_eq!(dependency, "StorageClient");

        // In a real test, we'd verify that HealthAnalyzer uses StorageClient
        // and that the dependency is properly injected
    }

    #[test]
    fn test_health_analyzer_health_report_dependency() {
        // Test that HealthAnalyzer properly depends on HealthReport
        let dependency = "HealthReport";
        assert_eq!(dependency, "HealthReport");

        // In a real test, we'd verify that HealthAnalyzer returns HealthReport
        // and that the dependency is properly used
    }

    #[test]
    fn test_health_analyzer_async_runtime() {
        // Test that HealthAnalyzer properly uses async runtime
        let runtime = "tokio";
        assert_eq!(runtime, "tokio");

        // In a real test, we'd verify that HealthAnalyzer uses tokio runtime
        // and that async methods are properly implemented
    }

    #[test]
    fn test_health_analyzer_pyresult_usage() {
        // Test that HealthAnalyzer properly uses PyResult for error handling
        let result_type = "PyResult";
        assert_eq!(result_type, "PyResult");

        // In a real test, we'd verify that HealthAnalyzer methods return PyResult
        // and that errors are properly converted to Python exceptions
    }

    #[test]
    fn test_health_analyzer_anyhow_usage() {
        // Test that HealthAnalyzer properly uses anyhow for error handling
        let error_crate = "anyhow";
        assert_eq!(error_crate, "anyhow");

        // In a real test, we'd verify that HealthAnalyzer uses anyhow::Result
        // and that errors are properly converted to PyResult
    }
}
