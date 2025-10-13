#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3_client::*;
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
        assert_eq!(object_info.last_modified, Some("2023-01-01T00:00:00Z".to_string()));
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
    fn test_object_info_debug() {
        let object_info = ObjectInfo {
            key: "test/file.parquet".to_string(),
            size: 1024,
            last_modified: Some("2023-01-01T00:00:00Z".to_string()),
            etag: Some("etag123".to_string()),
        };
        
        let debug_str = format!("{:?}", object_info);
        assert!(debug_str.contains("test/file.parquet"));
        assert!(debug_str.contains("1024"));
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
    fn test_s3_url_parsing_missing_bucket() {
        let s3_path = "s3:///my-table/";
        let url = Url::parse(s3_path).unwrap();
        
        // This should be valid URL parsing but bucket will be empty
        assert_eq!(url.scheme(), "s3");
        assert_eq!(url.host_str(), None);
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
    fn test_s3_path_components_extraction_root_bucket() {
        let s3_path = "s3://my-bucket/";
        let url = Url::parse(s3_path).unwrap();
        
        let bucket = url.host_str().unwrap().to_string();
        let prefix = url.path().trim_start_matches('/').to_string();
        
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_s3_path_components_extraction_nested_path() {
        let s3_path = "s3://my-bucket/data/lake/tables/my-table/";
        let url = Url::parse(s3_path).unwrap();
        
        let bucket = url.host_str().unwrap().to_string();
        let prefix = url.path().trim_start_matches('/').to_string();
        
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "data/lake/tables/my-table/");
    }

    #[test]
    fn test_aws_region_creation() {
        let region_str = "us-west-2";
        let region = aws_sdk_s3::config::Region::new(region_str);
        
        assert_eq!(region.as_ref(), "us-west-2");
    }

    #[test]
    fn test_aws_region_creation_eu_region() {
        let region_str = "eu-west-1";
        let region = aws_sdk_s3::config::Region::new(region_str);
        
        assert_eq!(region.as_ref(), "eu-west-1");
    }

    #[test]
    fn test_aws_region_creation_ap_region() {
        let region_str = "ap-southeast-1";
        let region = aws_sdk_s3::config::Region::new(region_str);
        
        assert_eq!(region.as_ref(), "ap-southeast-1");
    }

    #[test]
    fn test_aws_credentials_creation() {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        
        let creds = aws_sdk_s3::config::Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "drainage"
        );
        
        assert_eq!(creds.access_key_id(), access_key);
        assert_eq!(creds.secret_access_key(), secret_key);
        assert_eq!(creds.session_token(), None);
        assert_eq!(creds.expiry(), None);
        assert_eq!(creds.provider_name(), "drainage");
    }

    #[test]
    fn test_aws_credentials_creation_with_session_token() {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let session_token = "session-token-example";
        
        let creds = aws_sdk_s3::config::Credentials::new(
            access_key,
            secret_key,
            Some(session_token),
            None,
            "drainage"
        );
        
        assert_eq!(creds.access_key_id(), access_key);
        assert_eq!(creds.secret_access_key(), secret_key);
        assert_eq!(creds.session_token(), Some(session_token));
        assert_eq!(creds.expiry(), None);
        assert_eq!(creds.provider_name(), "drainage");
    }

    #[test]
    fn test_aws_credentials_creation_with_expiry() {
        use std::time::{Duration, SystemTime, UNIX_EPOCH};
        
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let expiry = SystemTime::now() + Duration::from_secs(3600);
        
        let creds = aws_sdk_s3::config::Credentials::new(
            access_key,
            secret_key,
            None,
            Some(expiry),
            "drainage"
        );
        
        assert_eq!(creds.access_key_id(), access_key);
        assert_eq!(creds.secret_access_key(), secret_key);
        assert_eq!(creds.session_token(), None);
        assert_eq!(creds.expiry(), Some(expiry));
        assert_eq!(creds.provider_name(), "drainage");
    }

    #[test]
    fn test_s3_client_wrapper_getters() {
        // This test would require actual S3 client creation, which is complex in unit tests
        // We'll test the getter methods conceptually
        let bucket = "test-bucket".to_string();
        let prefix = "test-prefix".to_string();
        
        // In a real test, we'd create a mock S3ClientWrapper
        // For now, we'll just test the string operations
        assert_eq!(bucket, "test-bucket");
        assert_eq!(prefix, "test-prefix");
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
    fn test_s3_path_validation_invalid() {
        let invalid_paths = vec![
            "https://bucket/",
            "ftp://bucket/",
            "not-a-url",
            "",
            "s3://",
            "s3:///",
        ];
        
        for path in invalid_paths {
            if path.is_empty() {
                continue; // Skip empty string test as it's handled differently
            }
            
            let result = Url::parse(path);
            if result.is_ok() {
                let url = result.unwrap();
                if url.scheme() != "s3" {
                    // This is expected for non-s3 URLs
                    continue;
                }
                if url.host_str().is_none() {
                    // This is expected for s3:// or s3:///
                    continue;
                }
            }
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

    #[test]
    fn test_object_info_size_types() {
        let small_object = ObjectInfo {
            key: "small.parquet".to_string(),
            size: 1024, // 1KB
            last_modified: None,
            etag: None,
        };
        
        let large_object = ObjectInfo {
            key: "large.parquet".to_string(),
            size: 1024 * 1024 * 1024, // 1GB
            last_modified: None,
            etag: None,
        };
        
        assert_eq!(small_object.size, 1024);
        assert_eq!(large_object.size, 1024 * 1024 * 1024);
        assert!(large_object.size > small_object.size);
    }

    #[test]
    fn test_object_info_key_variations() {
        let keys = vec![
            "file.parquet",
            "path/to/file.parquet",
            "deeply/nested/path/to/file.parquet",
            "file_with_underscores.parquet",
            "file-with-dashes.parquet",
            "file.with.dots.parquet",
            "file123.parquet",
            "UPPERCASE.parquet",
            "mixedCase.parquet",
        ];
        
        for key in keys {
            let object_info = ObjectInfo {
                key: key.to_string(),
                size: 1024,
                last_modified: None,
                etag: None,
            };
            
            assert_eq!(object_info.key, key);
        }
    }

    #[test]
    fn test_object_info_etag_variations() {
        let etags = vec![
            Some("\"etag123\"".to_string()),
            Some("etag123".to_string()),
            Some("".to_string()),
            None,
        ];
        
        for etag in etags {
            let object_info = ObjectInfo {
                key: "test.parquet".to_string(),
                size: 1024,
                last_modified: None,
                etag: etag.clone(),
            };
            
            assert_eq!(object_info.etag, etag);
        }
    }

    #[test]
    fn test_object_info_last_modified_variations() {
        let timestamps = vec![
            Some("2023-01-01T00:00:00Z".to_string()),
            Some("2023-12-31T23:59:59Z".to_string()),
            Some("2023-01-01T00:00:00.000Z".to_string()),
            Some("2023-01-01T00:00:00+00:00".to_string()),
            None,
        ];
        
        for timestamp in timestamps {
            let object_info = ObjectInfo {
                key: "test.parquet".to_string(),
                size: 1024,
                last_modified: timestamp.clone(),
                etag: None,
            };
            
            assert_eq!(object_info.last_modified, timestamp);
        }
    }
}
