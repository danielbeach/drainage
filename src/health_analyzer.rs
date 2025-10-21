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
#[path = "health_analyzer_tests.rs"]
mod health_analyzer_tests;
