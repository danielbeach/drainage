use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client as S3Client, config::Region, config::Credentials};
use anyhow::Result;
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
        let bucket = url.host_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid S3 URL: missing bucket"))?
            .to_string();
        let prefix = url.path().trim_start_matches('/').to_string();

        let region = if let Some(region_str) = aws_region {
            Region::new(region_str)
        } else {
            RegionProviderChain::default_provider().region().await.unwrap_or_else(|| Region::new("us-east-1"))
        };

        let config = if let (Some(access_key), Some(secret_key)) = (aws_access_key_id, aws_secret_access_key) {
            let creds = Credentials::new(access_key, secret_key, None, None, "drainage");
            aws_config::from_env()
                .region(region)
                .credentials_provider(creds)
                .load()
                .await
        } else {
            aws_config::from_env()
                .region(region)
                .load()
                .await
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
            let mut request = self.client
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
        let response = self.client
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
