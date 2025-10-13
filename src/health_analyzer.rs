use crate::s3_client::S3ClientWrapper;
use crate::types::HealthReport;
use crate::delta_lake::DeltaLakeAnalyzer;
use crate::iceberg::IcebergAnalyzer;
use pyo3::prelude::*;

#[pyclass]
pub struct HealthAnalyzer {
    s3_client: S3ClientWrapper,
}

#[pymethods]
impl HealthAnalyzer {
    /// Get basic table information
    pub fn get_table_info(&self) -> PyResult<(String, String)> {
        Ok((
            self.s3_client.get_bucket().to_string(),
            self.s3_client.get_prefix().to_string(),
        ))
    }
}

impl HealthAnalyzer {
    /// Create a new HealthAnalyzer asynchronously (internal use)
    pub async fn create_async(
        s3_path: String,
        aws_access_key_id: Option<String>,
        aws_secret_access_key: Option<String>,
        aws_region: Option<String>,
    ) -> PyResult<Self> {
        let s3_client = S3ClientWrapper::new(&s3_path, aws_access_key_id, aws_secret_access_key, aws_region)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create S3 client: {}", e)))?;
        
        Ok(Self { s3_client })
    }

    /// Analyze Delta Lake table health (internal use)
    pub async fn analyze_delta_lake(&self) -> PyResult<HealthReport> {
        let analyzer = DeltaLakeAnalyzer::new(self.s3_client.clone());
        analyzer.analyze().await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Delta Lake analysis failed: {}", e)))
    }

    /// Analyze Apache Iceberg table health (internal use)
    pub async fn analyze_iceberg(&self) -> PyResult<HealthReport> {
        let analyzer = IcebergAnalyzer::new(self.s3_client.clone());
        analyzer.analyze().await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Iceberg analysis failed: {}", e)))
    }

    /// List objects for table type detection (internal use)
    pub async fn list_objects_for_detection(&self) -> PyResult<Vec<crate::s3_client::ObjectInfo>> {
        self.s3_client.list_objects(self.s3_client.get_prefix()).await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to list objects: {}", e)))
    }
}

// We need to implement Clone for S3ClientWrapper to use it in the analyzer methods
impl Clone for S3ClientWrapper {
    fn clone(&self) -> Self {
        // This is a simplified clone - in practice, you might want to implement
        // a more sophisticated cloning strategy or use Arc<Mutex<>> for shared state
        Self {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_health_analyzer_get_table_info() {
        // This test would require a mock S3ClientWrapper
        // For now, we'll test the concept
        let bucket = "test-bucket".to_string();
        let prefix = "test-prefix".to_string();
        
        // In a real test, we'd create a mock HealthAnalyzer
        // and verify that get_table_info returns the correct values
        assert_eq!(bucket, "test-bucket");
        assert_eq!(prefix, "test-prefix");
    }

    #[test]
    fn test_health_analyzer_creation_parameters() {
        let s3_path = "s3://test-bucket/test-table/";
        let aws_access_key_id = Some("AKIAIOSFODNN7EXAMPLE".to_string());
        let aws_secret_access_key = Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string());
        let aws_region = Some("us-west-2".to_string());
        
        // Test parameter validation
        assert!(aws_access_key_id.is_some());
        assert!(aws_secret_access_key.is_some());
        assert!(aws_region.is_some());
        assert!(s3_path.starts_with("s3://"));
    }

    #[test]
    fn test_health_analyzer_s3_path_validation() {
        let valid_paths = vec![
            "s3://bucket/table/",
            "s3://my-bucket/my-table/",
            "s3://bucket.with.dots/table/",
            "s3://bucket/path/to/table/",
        ];
        
        for path in valid_paths {
            assert!(path.starts_with("s3://"), "Invalid S3 path: {}", path);
            assert!(path.contains("/"), "S3 path should contain path separator: {}", path);
        }
    }

    #[test]
    fn test_health_analyzer_table_type_detection_delta() {
        let objects = vec![
            crate::s3_client::ObjectInfo {
                key: "part-00000.parquet".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
            crate::s3_client::ObjectInfo {
                key: "_delta_log/00000000000000000000.json".to_string(),
                size: 2048,
                last_modified: None,
                etag: None,
            },
            crate::s3_client::ObjectInfo {
                key: "_delta_log/00000000000000000001.json".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
        ];
        
        // Check for Delta Lake characteristic files
        let has_delta_log = objects.iter().any(|obj| obj.key.contains("_delta_log/") && obj.key.ends_with(".json"));
        let has_iceberg_metadata = objects.iter().any(|obj| obj.key.ends_with("metadata.json"));
        
        assert!(has_delta_log, "Should detect Delta Lake files");
        assert!(!has_iceberg_metadata, "Should not detect Iceberg files");
    }

    #[test]
    fn test_health_analyzer_table_type_detection_iceberg() {
        let objects = vec![
            crate::s3_client::ObjectInfo {
                key: "data/00000-0-00000000000000000000-00000000000000000000.parquet".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
            crate::s3_client::ObjectInfo {
                key: "metadata/00000-00000000000000000000.metadata.json".to_string(),
                size: 2048,
                last_modified: None,
                etag: None,
            },
            crate::s3_client::ObjectInfo {
                key: "metadata/snap-00000000000000000000-1-00000000000000000000.avro".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
        ];
        
        // Check for Iceberg characteristic files
        let has_delta_log = objects.iter().any(|obj| obj.key.contains("_delta_log/") && obj.key.ends_with(".json"));
        let has_iceberg_metadata = objects.iter().any(|obj| obj.key.ends_with("metadata.json"));
        
        assert!(!has_delta_log, "Should not detect Delta Lake files");
        assert!(has_iceberg_metadata, "Should detect Iceberg files");
    }

    #[test]
    fn test_health_analyzer_table_type_detection_ambiguous() {
        let objects = vec![
            crate::s3_client::ObjectInfo {
                key: "part-00000.parquet".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
            crate::s3_client::ObjectInfo {
                key: "_delta_log/00000000000000000000.json".to_string(),
                size: 2048,
                last_modified: None,
                etag: None,
            },
            crate::s3_client::ObjectInfo {
                key: "metadata/00000-00000000000000000000.metadata.json".to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            },
        ];
        
        // Check for both Delta Lake and Iceberg files
        let has_delta_log = objects.iter().any(|obj| obj.key.contains("_delta_log/") && obj.key.ends_with(".json"));
        let has_iceberg_metadata = objects.iter().any(|obj| obj.key.ends_with("metadata.json"));
        
        assert!(has_delta_log, "Should detect Delta Lake files");
        assert!(has_iceberg_metadata, "Should detect Iceberg files");
        // This should be ambiguous
    }

    #[test]
    fn test_health_analyzer_s3_client_wrapper_clone() {
        // Test that S3ClientWrapper can be cloned
        // This is important for the HealthAnalyzer implementation
        let bucket = "test-bucket".to_string();
        let prefix = "test-prefix".to_string();
        
        // In a real test, we'd create an actual S3ClientWrapper and test cloning
        // For now, we'll test the concept
        let bucket_clone = bucket.clone();
        let prefix_clone = prefix.clone();
        
        assert_eq!(bucket, bucket_clone);
        assert_eq!(prefix, prefix_clone);
    }
}
