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
#[path = "storage_client_tests.rs"]
mod storage_client_tests;
