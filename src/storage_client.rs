use anyhow::Result;
use bytes::Bytes;
use futures::stream::StreamExt;
use object_store::{aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, ObjectStore};
use std::sync::Arc;
use url::Url;

pub struct StorageClient {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    prefix: String,
}

// Implement Clone for StorageClient
impl Clone for StorageClient {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
        }
    }
}

impl StorageClient {
    pub async fn new(
        storage_path: &str,
        // AWS credentials (for s3:// URLs)
        aws_access_key_id: Option<String>,
        aws_secret_access_key: Option<String>,
        aws_region: Option<String>,
        // GCS credentials (for gs:// URLs)
        gcs_service_account_key: Option<String>,
    ) -> Result<Self> {
        let url = Url::parse(storage_path)?;
        let bucket = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid storage URL: missing bucket"))?
            .to_string();
        let prefix = url.path().trim_start_matches('/').to_string();

        let scheme = url.scheme();

        let store: Arc<dyn ObjectStore> = match scheme {
            "s3" => {
                let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);

                // Set region
                if let Some(region) = aws_region {
                    builder = builder.with_region(&region);
                }

                // Set credentials if provided
                if let (Some(access_key), Some(secret_key)) =
                    (aws_access_key_id, aws_secret_access_key)
                {
                    builder = builder
                        .with_access_key_id(&access_key)
                        .with_secret_access_key(&secret_key);
                }

                Arc::new(builder.build()?)
            }
            "gs" => {
                let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);

                // Set service account key if provided
                if let Some(service_account_key) = gcs_service_account_key {
                    builder = builder.with_service_account_key(&service_account_key);
                }

                Arc::new(builder.build()?)
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported storage scheme: {}. Supported schemes: s3://, gs://",
                    scheme
                ));
            }
        };

        Ok(Self {
            store,
            bucket,
            prefix,
        })
    }

    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let list_prefix = if prefix.is_empty() {
            None
        } else {
            Some(object_store::path::Path::from(prefix))
        };

        let mut objects = Vec::new();
        let mut list_stream = self.store.list(list_prefix.as_ref());

        while let Some(meta_result) = list_stream.next().await {
            let meta = meta_result?;
            objects.push(ObjectInfo {
                key: meta.location.to_string(),
                size: meta.size as i64,
                last_modified: Some(meta.last_modified.to_rfc3339()),
                etag: meta.e_tag.clone(),
            });
        }

        Ok(objects)
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let path = object_store::path::Path::from(key);
        let get_result = self.store.get(&path).await?;
        let bytes: Bytes = get_result.bytes().await?;
        Ok(bytes.to_vec())
    }

    pub fn get_bucket(&self) -> &str {
        &self.bucket
    }

    pub fn get_prefix(&self) -> &str {
        &self.prefix
    }
}

#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: i64,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::storage_client::ObjectInfo;
    use url::Url;

    #[test]
    fn test_object_info_creation() {
        let object_info = ObjectInfo {
            key: "test/file.parquet".to_string(),
            size: 1024,
            last_modified: Some("2023-01-01T00:00:00Z".to_string()),
            etag: Some("etag123".to_string()),
        };

        assert_eq!(object_info.key, "test/file.parquet");
        assert_eq!(object_info.size, 1024);
        assert_eq!(
            object_info.last_modified,
            Some("2023-01-01T00:00:00Z".to_string())
        );
        assert_eq!(object_info.etag, Some("etag123".to_string()));
    }

    #[test]
    fn test_object_info_clone() {
        let object_info = ObjectInfo {
            key: "test/file.parquet".to_string(),
            size: 1024,
            last_modified: Some("2023-01-01T00:00:00Z".to_string()),
            etag: Some("etag123".to_string()),
        };

        let cloned = object_info.clone();
        assert_eq!(cloned.key, object_info.key);
        assert_eq!(cloned.size, object_info.size);
        assert_eq!(cloned.last_modified, object_info.last_modified);
        assert_eq!(cloned.etag, object_info.etag);
    }

    #[test]
    fn test_s3_url_parsing_valid() {
        let s3_path = "s3://my-bucket/my-table/";
        let url = Url::parse(s3_path).unwrap();

        assert_eq!(url.scheme(), "s3");
        assert_eq!(url.host_str(), Some("my-bucket"));
        assert_eq!(url.path(), "/my-table/");
    }

    #[test]
    fn test_gcs_url_parsing_valid() {
        let gcs_path = "gs://my-bucket/my-table/";
        let url = Url::parse(gcs_path).unwrap();

        assert_eq!(url.scheme(), "gs");
        assert_eq!(url.host_str(), Some("my-bucket"));
        assert_eq!(url.path(), "/my-table/");
    }

    #[test]
    fn test_s3_url_parsing_with_prefix() {
        let s3_path = "s3://my-bucket/my-table/year=2023/month=01/";
        let url = Url::parse(s3_path).unwrap();

        assert_eq!(url.scheme(), "s3");
        assert_eq!(url.host_str(), Some("my-bucket"));
        assert_eq!(url.path(), "/my-table/year=2023/month=01/");
    }

    #[test]
    fn test_gcs_url_parsing_with_prefix() {
        let gcs_path = "gs://my-bucket/my-table/year=2023/month=01/";
        let url = Url::parse(gcs_path).unwrap();

        assert_eq!(url.scheme(), "gs");
        assert_eq!(url.host_str(), Some("my-bucket"));
        assert_eq!(url.path(), "/my-table/year=2023/month=01/");
    }

    #[test]
    fn test_url_parsing_invalid() {
        let invalid_path = "not-a-url";
        let result = Url::parse(invalid_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_path_components_extraction() {
        let s3_path = "s3://my-bucket/my-table/year=2023/month=01/";
        let url = Url::parse(s3_path).unwrap();

        let bucket = url.host_str().unwrap().to_string();
        let prefix = url.path().trim_start_matches('/').to_string();

        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "my-table/year=2023/month=01/");
    }

    #[test]
    fn test_gcs_path_components_extraction() {
        let gcs_path = "gs://my-bucket/my-table/year=2023/month=01/";
        let url = Url::parse(gcs_path).unwrap();

        let bucket = url.host_str().unwrap().to_string();
        let prefix = url.path().trim_start_matches('/').to_string();

        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "my-table/year=2023/month=01/");
    }

    #[test]
    fn test_path_components_extraction_no_trailing_slash() {
        let s3_path = "s3://my-bucket/my-table";
        let url = Url::parse(s3_path).unwrap();

        let bucket = url.host_str().unwrap().to_string();
        let prefix = url.path().trim_start_matches('/').to_string();

        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "my-table");
    }

    #[test]
    fn test_storage_path_validation() {
        let valid_paths = vec![
            "s3://bucket/",
            "s3://bucket/path/",
            "s3://bucket/path/to/table/",
            "s3://my-bucket-name/my-table/",
            "s3://bucket.with.dots/table/",
            "gs://bucket/",
            "gs://bucket/path/",
            "gs://bucket/path/to/table/",
            "gs://my-bucket-name/my-table/",
        ];

        for path in valid_paths {
            let result = Url::parse(path);
            assert!(
                result.is_ok(),
                "Failed to parse valid storage path: {}",
                path
            );

            let url = result.unwrap();
            assert!(
                url.scheme() == "s3" || url.scheme() == "gs",
                "Invalid scheme for path: {}",
                path
            );
            assert!(url.host_str().is_some(), "Missing bucket in path: {}", path);
        }
    }

    #[test]
    fn test_object_info_optional_fields() {
        let object_info_with_all = ObjectInfo {
            key: "test/file.parquet".to_string(),
            size: 1024,
            last_modified: Some("2023-01-01T00:00:00Z".to_string()),
            etag: Some("etag123".to_string()),
        };

        let object_info_minimal = ObjectInfo {
            key: "test/file.parquet".to_string(),
            size: 1024,
            last_modified: None,
            etag: None,
        };

        assert!(object_info_with_all.last_modified.is_some());
        assert!(object_info_with_all.etag.is_some());
        assert!(object_info_minimal.last_modified.is_none());
        assert!(object_info_minimal.etag.is_none());
    }
}
