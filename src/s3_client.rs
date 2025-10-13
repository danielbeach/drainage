use anyhow::Result;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Credentials, config::Region, Client as S3Client};
use url::Url;

pub struct S3ClientWrapper {
    pub client: S3Client,
    pub bucket: String,
    pub prefix: String,
}

impl S3ClientWrapper {
    pub async fn new(
        s3_path: &str,
        aws_access_key_id: Option<String>,
        aws_secret_access_key: Option<String>,
        aws_region: Option<String>,
    ) -> Result<Self> {
        let url = Url::parse(s3_path)?;
        let bucket = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid S3 URL: missing bucket"))?
            .to_string();
        let prefix = url.path().trim_start_matches('/').to_string();

        let region = if let Some(region_str) = aws_region {
            Region::new(region_str)
        } else {
            RegionProviderChain::default_provider()
                .region()
                .await
                .unwrap_or_else(|| Region::new("us-east-1"))
        };

        let config = if let (Some(access_key), Some(secret_key)) =
            (aws_access_key_id, aws_secret_access_key)
        {
            let creds = Credentials::new(access_key, secret_key, None, None, "drainage");
            aws_config::from_env()
                .region(region)
                .credentials_provider(creds)
                .load()
                .await
        } else {
            aws_config::from_env().region(region).load().await
        };

        let client = S3Client::new(&config);

        Ok(Self {
            client,
            bucket,
            prefix,
        })
    }

    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let mut objects = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    objects.push(ObjectInfo {
                        key: obj.key.unwrap_or_default(),
                        size: obj.size,
                        last_modified: obj.last_modified.map(|dt| format!("{:?}", dt)),
                        etag: obj.e_tag,
                    });
                }
            }

            if response.is_truncated {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(objects)
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let body = response.body.collect().await?.into_bytes().to_vec();
        Ok(body)
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
    use super::*;
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
    fn test_s3_url_parsing_with_prefix() {
        let s3_path = "s3://my-bucket/my-table/year=2023/month=01/";
        let url = Url::parse(s3_path).unwrap();

        assert_eq!(url.scheme(), "s3");
        assert_eq!(url.host_str(), Some("my-bucket"));
        assert_eq!(url.path(), "/my-table/year=2023/month=01/");
    }

    #[test]
    fn test_s3_url_parsing_invalid() {
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
    fn test_s3_path_components_extraction_no_trailing_slash() {
        let s3_path = "s3://my-bucket/my-table";
        let url = Url::parse(s3_path).unwrap();

        let bucket = url.host_str().unwrap().to_string();
        let prefix = url.path().trim_start_matches('/').to_string();

        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "my-table");
    }

    #[test]
    fn test_aws_region_creation() {
        let region_str = "us-west-2";
        let region = aws_sdk_s3::config::Region::new(region_str);

        assert_eq!(region.as_ref(), "us-west-2");
    }

    #[test]
    fn test_aws_credentials_creation() {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        let creds =
            aws_sdk_s3::config::Credentials::new(access_key, secret_key, None, None, "drainage");

        assert_eq!(creds.access_key_id(), access_key);
        assert_eq!(creds.secret_access_key(), secret_key);
        assert_eq!(creds.session_token(), None);
        assert_eq!(creds.expiry(), None);
    }

    #[test]
    fn test_s3_path_validation() {
        let valid_paths = vec![
            "s3://bucket/",
            "s3://bucket/path/",
            "s3://bucket/path/to/table/",
            "s3://my-bucket-name/my-table/",
            "s3://bucket.with.dots/table/",
        ];

        for path in valid_paths {
            let result = Url::parse(path);
            assert!(result.is_ok(), "Failed to parse valid S3 path: {}", path);

            let url = result.unwrap();
            assert_eq!(url.scheme(), "s3");
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
