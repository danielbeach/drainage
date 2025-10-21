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
