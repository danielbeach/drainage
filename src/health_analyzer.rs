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
