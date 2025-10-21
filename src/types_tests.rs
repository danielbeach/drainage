#[cfg(test)]
mod tests {
    use crate::types::*;
    use std::collections::HashMap;

    #[test]
    fn test_health_metrics_new() {
        let metrics = HealthMetrics::new();

        assert_eq!(metrics.total_files, 0);
        assert_eq!(metrics.total_size_bytes, 0);
        assert_eq!(metrics.unreferenced_files.len(), 0);
        assert_eq!(metrics.unreferenced_size_bytes, 0);
        assert_eq!(metrics.partition_count, 0);
        assert_eq!(metrics.partitions.len(), 0);
        assert!(metrics.clustering.is_none());
        assert_eq!(metrics.avg_file_size_bytes, 0.0);
        assert_eq!(metrics.file_size_distribution.small_files, 0);
        assert_eq!(metrics.file_size_distribution.medium_files, 0);
        assert_eq!(metrics.file_size_distribution.large_files, 0);
        assert_eq!(metrics.file_size_distribution.very_large_files, 0);
        assert_eq!(metrics.recommendations.len(), 0);
        assert_eq!(metrics.health_score, 0.0);
    }

    #[test]
    fn test_health_score_calculation_perfect_health() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 100,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };

        let score = metrics.calculate_health_score();
        assert!(
            (score - 1.0).abs() < 0.01,
            "Expected perfect health score, got {}",
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_unreferenced_files() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.unreferenced_files = vec![
            FileInfo {
                path: "unreferenced1.parquet".to_string(),
                size_bytes: 1000,
                last_modified: None,
                is_referenced: false,
            },
            FileInfo {
                path: "unreferenced2.parquet".to_string(),
                size_bytes: 2000,
                last_modified: None,
                is_referenced: false,
            },
        ];
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 100,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };

        let score = metrics.calculate_health_score();
        // Should be penalized by 2% (2 unreferenced files out of 100 total)
        let expected_penalty = 0.02 * 0.3; // 2% * 30% penalty
        let expected_score = 1.0 - expected_penalty;
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_small_files() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 50, // 50% small files
            medium_files: 50,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };

        let score = metrics.calculate_health_score();
        // Should be penalized by 10% (50% small files * 20% penalty)
        let expected_penalty = 0.5 * 0.2;
        let expected_score = 1.0 - expected_penalty;
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_very_large_files() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 90,
            large_files: 0,
            very_large_files: 10, // 10% very large files
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };

        let score = metrics.calculate_health_score();
        // Should be penalized by 1% (10% very large files * 10% penalty)
        let expected_penalty = 0.1 * 0.1;
        let expected_score = 1.0 - expected_penalty;
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_data_skew() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 100,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.5, // 50% skew
            file_size_skew_score: 0.3, // 30% skew
            largest_partition_size: 2000,
            smallest_partition_size: 1000,
            avg_partition_size: 1500,
            partition_size_std_dev: 500.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };

        let score = metrics.calculate_health_score();
        // Should be penalized by 10.5% (0.5 * 0.15 + 0.3 * 0.1)
        let expected_penalty = 0.5 * 0.15 + 0.3 * 0.1;
        let expected_score = 1.0 - expected_penalty;
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_metadata_bloat() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 100,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.metadata_health = MetadataHealth {
            metadata_file_count: 10,
            metadata_total_size_bytes: 200 * 1024 * 1024, // 200MB > 100MB threshold
            avg_metadata_file_size: 20.0 * 1024.0 * 1024.0,
            metadata_growth_rate: 0.0,
            manifest_file_count: 0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };

        let score = metrics.calculate_health_score();
        // Should be penalized by 5% for metadata bloat
        let expected_score = 1.0 - 0.05;
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_snapshot_retention_risk() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 100,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 150, // High snapshot count
            oldest_snapshot_age_days: 30.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 15.0,
            snapshot_retention_risk: 0.8, // High retention risk
        };

        let score = metrics.calculate_health_score();
        // Should be penalized by 8% for snapshot retention risk
        let expected_score = 1.0 - 0.8 * 0.1;
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_deletion_vector_impact() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 100,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };
        metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
            deletion_vector_count: 10,
            total_deletion_vector_size_bytes: 1024 * 1024,
            avg_deletion_vector_size_bytes: 102.4 * 1024.0,
            deletion_vector_age_days: 5.0,
            deleted_rows_count: 1000,
            deletion_vector_impact_score: 0.6, // High impact
        });

        let score = metrics.calculate_health_score();
        // Should be penalized by 9% for deletion vector impact
        let expected_score = 1.0 - 0.6 * 0.15;
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_schema_instability() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 100,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };
        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: 20,
            breaking_changes: 5,
            non_breaking_changes: 15,
            schema_stability_score: 0.3, // Low stability
            days_since_last_change: 1.0,
            schema_change_frequency: 0.1,
            current_schema_version: 20,
        });

        let score = metrics.calculate_health_score();
        // Should be penalized by 14% for schema instability (1.0 - 0.3) * 0.2
        let expected_score = 1.0 - (1.0 - 0.3) * 0.2;
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_time_travel_costs() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 100,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };
        metrics.time_travel_metrics = Some(TimeTravelMetrics {
            total_snapshots: 100,
            oldest_snapshot_age_days: 30.0,
            newest_snapshot_age_days: 0.0,
            total_historical_size_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            avg_snapshot_size_bytes: 100.0 * 1024.0 * 1024.0,
            storage_cost_impact_score: 0.7,  // High cost impact
            retention_efficiency_score: 0.4, // Low efficiency
            recommended_retention_days: 7,
        });

        let score = metrics.calculate_health_score();
        // Should be penalized by 10.5% (0.7 * 0.1 + (1.0 - 0.4) * 0.05)
        let expected_score = 1.0 - (0.7 * 0.1 + (1.0 - 0.4) * 0.05);
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_data_quality_issues() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 100,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };
        metrics.table_constraints = Some(TableConstraintsMetrics {
            total_constraints: 5,
            check_constraints: 2,
            not_null_constraints: 2,
            unique_constraints: 1,
            foreign_key_constraints: 0,
            constraint_violation_risk: 0.8, // High violation risk
            data_quality_score: 0.2,        // Poor data quality
            constraint_coverage_score: 0.3, // Low coverage
        });

        let score = metrics.calculate_health_score();
        // Should be penalized by 22% ((1.0 - 0.2) * 0.15 + 0.8 * 0.1)
        let expected_score = 1.0 - ((1.0 - 0.2) * 0.15 + 0.8 * 0.1);
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_with_compaction_opportunities() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 0,
            medium_files: 100,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 10;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 0.0,
            file_size_skew_score: 0.0,
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 5,
            oldest_snapshot_age_days: 1.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 0.5,
            snapshot_retention_risk: 0.0,
        };
        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: 0.9, // High opportunity
            small_files_count: 50,
            small_files_size_bytes: 100 * 1024 * 1024,
            potential_compaction_files: 50,
            estimated_compaction_savings_bytes: 20 * 1024 * 1024,
            recommended_target_file_size_bytes: 128 * 1024 * 1024,
            compaction_priority: "high".to_string(),
            z_order_opportunity: true,
            z_order_columns: vec!["col1".to_string(), "col2".to_string()],
        });

        let score = metrics.calculate_health_score();
        // Should be penalized by 1% for compaction opportunities (1.0 - 0.9) * 0.1
        let expected_score = 1.0 - (1.0 - 0.9) * 0.1;
        assert!(
            (score - expected_score).abs() < 0.01,
            "Expected score ~{}, got {}",
            expected_score,
            score
        );
    }

    #[test]
    fn test_health_score_calculation_minimum_score() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.unreferenced_files = vec![
            FileInfo {
                path: "unreferenced.parquet".to_string(),
                size_bytes: 1000,
                last_modified: None,
                is_referenced: false,
            };
            100
        ]; // All files unreferenced
        metrics.file_size_distribution = FileSizeDistribution {
            small_files: 100, // All small files
            medium_files: 0,
            large_files: 0,
            very_large_files: 0,
        };
        metrics.partition_count = 1;
        metrics.data_skew = DataSkewMetrics {
            partition_skew_score: 1.0, // Maximum skew
            file_size_skew_score: 1.0, // Maximum skew
            largest_partition_size: 1000,
            smallest_partition_size: 1000,
            avg_partition_size: 1000,
            partition_size_std_dev: 0.0,
        };
        metrics.snapshot_health = SnapshotHealth {
            snapshot_count: 1000,
            oldest_snapshot_age_days: 365.0,
            newest_snapshot_age_days: 0.0,
            avg_snapshot_age_days: 182.5,
            snapshot_retention_risk: 1.0, // Maximum risk
        };

        let score = metrics.calculate_health_score();
        // Should be close to 0 but not negative
        assert!(
            score >= 0.0,
            "Health score should not be negative, got {}",
            score
        );
        assert!(
            score < 0.2,
            "Health score should be very low, got {}",
            score
        );
    }

    #[test]
    fn test_calculate_data_skew_empty_partitions() {
        let mut metrics = HealthMetrics::new();
        metrics.partitions = vec![];

        metrics.calculate_data_skew();

        // Should not crash and should keep default values
        assert_eq!(metrics.data_skew.partition_skew_score, 0.0);
        assert_eq!(metrics.data_skew.file_size_skew_score, 0.0);
    }

    #[test]
    fn test_calculate_data_skew_perfect_distribution() {
        let mut metrics = HealthMetrics::new();
        metrics.partitions = vec![
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 10,
                total_size_bytes: 1000,
                avg_file_size_bytes: 100.0,
                files: vec![],
            },
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 10,
                total_size_bytes: 1000,
                avg_file_size_bytes: 100.0,
                files: vec![],
            },
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 10,
                total_size_bytes: 1000,
                avg_file_size_bytes: 100.0,
                files: vec![],
            },
        ];

        metrics.calculate_data_skew();

        // Perfect distribution should have 0 skew
        assert_eq!(metrics.data_skew.partition_skew_score, 0.0);
        assert_eq!(metrics.data_skew.file_size_skew_score, 0.0);
        assert_eq!(metrics.data_skew.largest_partition_size, 1000);
        assert_eq!(metrics.data_skew.smallest_partition_size, 1000);
        assert_eq!(metrics.data_skew.avg_partition_size, 1000);
    }

    #[test]
    fn test_calculate_data_skew_high_skew() {
        let mut metrics = HealthMetrics::new();
        metrics.partitions = vec![
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 1,
                total_size_bytes: 100,
                avg_file_size_bytes: 100.0,
                files: vec![],
            },
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 99,
                total_size_bytes: 9900,
                avg_file_size_bytes: 100.0,
                files: vec![],
            },
        ];

        metrics.calculate_data_skew();

        // High skew should result in high skew scores
        assert!(metrics.data_skew.partition_skew_score > 0.5);
        assert!(metrics.data_skew.file_size_skew_score > 0.5);
        assert_eq!(metrics.data_skew.largest_partition_size, 9900);
        assert_eq!(metrics.data_skew.smallest_partition_size, 100);
    }

    #[test]
    fn test_calculate_metadata_health() {
        let mut metrics = HealthMetrics::new();
        let metadata_files = vec![
            crate::storage_client::ObjectInfo {
                key: "metadata1.json".to_string(),
                size: 1000,
                last_modified: Some("2023-01-01T00:00:00Z".to_string()),
                etag: Some("etag1".to_string()),
            },
            crate::storage_client::ObjectInfo {
                key: "metadata2.json".to_string(),
                size: 2000,
                last_modified: Some("2023-01-02T00:00:00Z".to_string()),
                etag: Some("etag2".to_string()),
            },
        ];

        metrics.calculate_metadata_health(&metadata_files);

        assert_eq!(metrics.metadata_health.metadata_file_count, 2);
        assert_eq!(metrics.metadata_health.metadata_total_size_bytes, 3000);
        assert_eq!(metrics.metadata_health.avg_metadata_file_size, 1500.0);
    }

    #[test]
    fn test_calculate_snapshot_health_low_risk() {
        let mut metrics = HealthMetrics::new();

        metrics.calculate_snapshot_health(5);

        assert_eq!(metrics.snapshot_health.snapshot_count, 5);
        assert_eq!(metrics.snapshot_health.snapshot_retention_risk, 0.0);
    }

    #[test]
    fn test_calculate_snapshot_health_medium_risk() {
        let mut metrics = HealthMetrics::new();

        metrics.calculate_snapshot_health(30);

        assert_eq!(metrics.snapshot_health.snapshot_count, 30);
        assert_eq!(metrics.snapshot_health.snapshot_retention_risk, 0.2);
    }

    #[test]
    fn test_calculate_snapshot_health_high_risk() {
        let mut metrics = HealthMetrics::new();

        metrics.calculate_snapshot_health(75);

        assert_eq!(metrics.snapshot_health.snapshot_count, 75);
        assert_eq!(metrics.snapshot_health.snapshot_retention_risk, 0.5);
    }

    #[test]
    fn test_calculate_snapshot_health_critical_risk() {
        let mut metrics = HealthMetrics::new();

        metrics.calculate_snapshot_health(150);

        assert_eq!(metrics.snapshot_health.snapshot_count, 150);
        assert_eq!(metrics.snapshot_health.snapshot_retention_risk, 0.8);
    }

    #[test]
    fn test_health_report_new() {
        let report = HealthReport::new("s3://bucket/table".to_string(), "delta".to_string());

        assert_eq!(report.table_path, "s3://bucket/table");
        assert_eq!(report.table_type, "delta");
        assert!(!report.analysis_timestamp.is_empty());
        assert_eq!(report.health_score, 0.0);
        assert_eq!(report.metrics.total_files, 0);
    }

    #[test]
    fn test_file_info_creation() {
        let file_info = FileInfo {
            path: "test.parquet".to_string(),
            size_bytes: 1024,
            last_modified: Some("2023-01-01T00:00:00Z".to_string()),
            is_referenced: true,
        };

        assert_eq!(file_info.path, "test.parquet");
        assert_eq!(file_info.size_bytes, 1024);
        assert_eq!(
            file_info.last_modified,
            Some("2023-01-01T00:00:00Z".to_string())
        );
        assert!(file_info.is_referenced);
    }

    #[test]
    fn test_partition_info_creation() {
        let mut partition_values = HashMap::new();
        partition_values.insert("year".to_string(), "2023".to_string());
        partition_values.insert("month".to_string(), "01".to_string());

        let partition_info = PartitionInfo {
            partition_values: partition_values.clone(),
            file_count: 10,
            total_size_bytes: 10000,
            avg_file_size_bytes: 1000.0,
            files: vec![],
        };

        assert_eq!(partition_info.partition_values, partition_values);
        assert_eq!(partition_info.file_count, 10);
        assert_eq!(partition_info.total_size_bytes, 10000);
        assert_eq!(partition_info.avg_file_size_bytes, 1000.0);
        assert_eq!(partition_info.files.len(), 0);
    }

    #[test]
    fn test_clustering_info_creation() {
        let clustering_info = ClusteringInfo {
            clustering_columns: vec!["col1".to_string(), "col2".to_string()],
            cluster_count: 5,
            avg_files_per_cluster: 20.0,
            avg_cluster_size_bytes: 2000.0,
        };

        assert_eq!(clustering_info.clustering_columns, vec!["col1", "col2"]);
        assert_eq!(clustering_info.cluster_count, 5);
        assert_eq!(clustering_info.avg_files_per_cluster, 20.0);
        assert_eq!(clustering_info.avg_cluster_size_bytes, 2000.0);
    }

    #[test]
    fn test_file_size_distribution_creation() {
        let distribution = FileSizeDistribution {
            small_files: 10,
            medium_files: 20,
            large_files: 5,
            very_large_files: 1,
        };

        assert_eq!(distribution.small_files, 10);
        assert_eq!(distribution.medium_files, 20);
        assert_eq!(distribution.large_files, 5);
        assert_eq!(distribution.very_large_files, 1);
    }

    #[test]
    fn test_deletion_vector_metrics_creation() {
        let dv_metrics = DeletionVectorMetrics {
            deletion_vector_count: 5,
            total_deletion_vector_size_bytes: 1024 * 1024,
            avg_deletion_vector_size_bytes: 204.8 * 1024.0,
            deletion_vector_age_days: 10.0,
            deleted_rows_count: 1000,
            deletion_vector_impact_score: 0.5,
        };

        assert_eq!(dv_metrics.deletion_vector_count, 5);
        assert_eq!(dv_metrics.total_deletion_vector_size_bytes, 1024 * 1024);
        assert_eq!(dv_metrics.avg_deletion_vector_size_bytes, 204.8 * 1024.0);
        assert_eq!(dv_metrics.deletion_vector_age_days, 10.0);
        assert_eq!(dv_metrics.deleted_rows_count, 1000);
        assert_eq!(dv_metrics.deletion_vector_impact_score, 0.5);
    }

    #[test]
    fn test_schema_evolution_metrics_creation() {
        let schema_metrics = SchemaEvolutionMetrics {
            total_schema_changes: 10,
            breaking_changes: 2,
            non_breaking_changes: 8,
            schema_stability_score: 0.8,
            days_since_last_change: 5.0,
            schema_change_frequency: 0.1,
            current_schema_version: 10,
        };

        assert_eq!(schema_metrics.total_schema_changes, 10);
        assert_eq!(schema_metrics.breaking_changes, 2);
        assert_eq!(schema_metrics.non_breaking_changes, 8);
        assert_eq!(schema_metrics.schema_stability_score, 0.8);
        assert_eq!(schema_metrics.days_since_last_change, 5.0);
        assert_eq!(schema_metrics.schema_change_frequency, 0.1);
        assert_eq!(schema_metrics.current_schema_version, 10);
    }

    #[test]
    fn test_time_travel_metrics_creation() {
        let tt_metrics = TimeTravelMetrics {
            total_snapshots: 50,
            oldest_snapshot_age_days: 30.0,
            newest_snapshot_age_days: 0.0,
            total_historical_size_bytes: 5 * 1024 * 1024 * 1024,
            avg_snapshot_size_bytes: 100.0 * 1024.0 * 1024.0,
            storage_cost_impact_score: 0.3,
            retention_efficiency_score: 0.7,
            recommended_retention_days: 14,
        };

        assert_eq!(tt_metrics.total_snapshots, 50);
        assert_eq!(tt_metrics.oldest_snapshot_age_days, 30.0);
        assert_eq!(tt_metrics.newest_snapshot_age_days, 0.0);
        assert_eq!(
            tt_metrics.total_historical_size_bytes,
            5 * 1024 * 1024 * 1024
        );
        assert_eq!(tt_metrics.avg_snapshot_size_bytes, 100.0 * 1024.0 * 1024.0);
        assert_eq!(tt_metrics.storage_cost_impact_score, 0.3);
        assert_eq!(tt_metrics.retention_efficiency_score, 0.7);
        assert_eq!(tt_metrics.recommended_retention_days, 14);
    }

    #[test]
    fn test_table_constraints_metrics_creation() {
        let constraint_metrics = TableConstraintsMetrics {
            total_constraints: 8,
            check_constraints: 3,
            not_null_constraints: 4,
            unique_constraints: 1,
            foreign_key_constraints: 0,
            constraint_violation_risk: 0.2,
            data_quality_score: 0.9,
            constraint_coverage_score: 0.8,
        };

        assert_eq!(constraint_metrics.total_constraints, 8);
        assert_eq!(constraint_metrics.check_constraints, 3);
        assert_eq!(constraint_metrics.not_null_constraints, 4);
        assert_eq!(constraint_metrics.unique_constraints, 1);
        assert_eq!(constraint_metrics.foreign_key_constraints, 0);
        assert_eq!(constraint_metrics.constraint_violation_risk, 0.2);
        assert_eq!(constraint_metrics.data_quality_score, 0.9);
        assert_eq!(constraint_metrics.constraint_coverage_score, 0.8);
    }

    #[test]
    fn test_file_compaction_metrics_creation() {
        let compaction_metrics = FileCompactionMetrics {
            compaction_opportunity_score: 0.7,
            small_files_count: 25,
            small_files_size_bytes: 50 * 1024 * 1024,
            potential_compaction_files: 25,
            estimated_compaction_savings_bytes: 10 * 1024 * 1024,
            recommended_target_file_size_bytes: 128 * 1024 * 1024,
            compaction_priority: "medium".to_string(),
            z_order_opportunity: true,
            z_order_columns: vec!["col1".to_string(), "col2".to_string()],
        };

        assert_eq!(compaction_metrics.compaction_opportunity_score, 0.7);
        assert_eq!(compaction_metrics.small_files_count, 25);
        assert_eq!(compaction_metrics.small_files_size_bytes, 50 * 1024 * 1024);
        assert_eq!(compaction_metrics.potential_compaction_files, 25);
        assert_eq!(
            compaction_metrics.estimated_compaction_savings_bytes,
            10 * 1024 * 1024
        );
        assert_eq!(
            compaction_metrics.recommended_target_file_size_bytes,
            128 * 1024 * 1024
        );
        assert_eq!(compaction_metrics.compaction_priority, "medium");
        assert!(compaction_metrics.z_order_opportunity);
        assert_eq!(compaction_metrics.z_order_columns, vec!["col1", "col2"]);
    }
}
