use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct FileInfo {
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub size_bytes: u64,
    #[pyo3(get)]
    pub last_modified: Option<String>,
    #[pyo3(get)]
    pub is_referenced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct PartitionInfo {
    #[pyo3(get)]
    pub partition_values: HashMap<String, String>,
    #[pyo3(get)]
    pub file_count: usize,
    #[pyo3(get)]
    pub total_size_bytes: u64,
    #[pyo3(get)]
    pub avg_file_size_bytes: f64,
    #[pyo3(get)]
    pub files: Vec<FileInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct ClusteringInfo {
    #[pyo3(get)]
    pub clustering_columns: Vec<String>,
    #[pyo3(get)]
    pub cluster_count: usize,
    #[pyo3(get)]
    pub avg_files_per_cluster: f64,
    #[pyo3(get)]
    pub avg_cluster_size_bytes: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct HealthMetrics {
    #[pyo3(get)]
    pub total_files: usize,
    #[pyo3(get)]
    pub total_size_bytes: u64,
    #[pyo3(get)]
    pub unreferenced_files: Vec<FileInfo>,
    #[pyo3(get)]
    pub unreferenced_size_bytes: u64,
    #[pyo3(get)]
    pub partition_count: usize,
    #[pyo3(get)]
    pub partitions: Vec<PartitionInfo>,
    #[pyo3(get)]
    pub clustering: Option<ClusteringInfo>,
    #[pyo3(get)]
    pub avg_file_size_bytes: f64,
    #[pyo3(get)]
    pub file_size_distribution: FileSizeDistribution,
    #[pyo3(get)]
    pub recommendations: Vec<String>,
    #[pyo3(get)]
    pub health_score: f64,
    #[pyo3(get)]
    pub data_skew: DataSkewMetrics,
    #[pyo3(get)]
    pub metadata_health: MetadataHealth,
    #[pyo3(get)]
    pub snapshot_health: SnapshotHealth,
    #[pyo3(get)]
    pub deletion_vector_metrics: Option<DeletionVectorMetrics>,
    #[pyo3(get)]
    pub schema_evolution: Option<SchemaEvolutionMetrics>,
    #[pyo3(get)]
    pub time_travel_metrics: Option<TimeTravelMetrics>,
    #[pyo3(get)]
    pub table_constraints: Option<TableConstraintsMetrics>,
    #[pyo3(get)]
    pub file_compaction: Option<FileCompactionMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct FileSizeDistribution {
    #[pyo3(get)]
    pub small_files: usize,  // < 16MB
    #[pyo3(get)]
    pub medium_files: usize, // 16MB - 128MB
    #[pyo3(get)]
    pub large_files: usize,  // 128MB - 1GB
    #[pyo3(get)]
    pub very_large_files: usize, // > 1GB
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct DataSkewMetrics {
    #[pyo3(get)]
    pub partition_skew_score: f64, // 0.0 (perfect) to 1.0 (highly skewed)
    #[pyo3(get)]
    pub file_size_skew_score: f64, // 0.0 (perfect) to 1.0 (highly skewed)
    #[pyo3(get)]
    pub largest_partition_size: u64,
    #[pyo3(get)]
    pub smallest_partition_size: u64,
    #[pyo3(get)]
    pub avg_partition_size: u64,
    #[pyo3(get)]
    pub partition_size_std_dev: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct MetadataHealth {
    #[pyo3(get)]
    pub metadata_file_count: usize,
    #[pyo3(get)]
    pub metadata_total_size_bytes: u64,
    #[pyo3(get)]
    pub avg_metadata_file_size: f64,
    #[pyo3(get)]
    pub metadata_growth_rate: f64, // bytes per day (estimated)
    #[pyo3(get)]
    pub manifest_file_count: usize, // For Iceberg
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct SnapshotHealth {
    #[pyo3(get)]
    pub snapshot_count: usize,
    #[pyo3(get)]
    pub oldest_snapshot_age_days: f64,
    #[pyo3(get)]
    pub newest_snapshot_age_days: f64,
    #[pyo3(get)]
    pub avg_snapshot_age_days: f64,
    #[pyo3(get)]
    pub snapshot_retention_risk: f64, // 0.0 (good) to 1.0 (high risk)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct HealthReport {
    #[pyo3(get)]
    pub table_path: String,
    #[pyo3(get)]
    pub table_type: String, // "delta" or "iceberg"
    #[pyo3(get)]
    pub analysis_timestamp: String,
    #[pyo3(get)]
    pub metrics: HealthMetrics,
    #[pyo3(get)]
    pub health_score: f64, // 0.0 to 1.0
}

impl HealthMetrics {
    pub fn new() -> Self {
        Self {
            total_files: 0,
            total_size_bytes: 0,
            unreferenced_files: Vec::new(),
            unreferenced_size_bytes: 0,
            partition_count: 0,
            partitions: Vec::new(),
            clustering: None,
            avg_file_size_bytes: 0.0,
            file_size_distribution: FileSizeDistribution {
                small_files: 0,
                medium_files: 0,
                large_files: 0,
                very_large_files: 0,
            },
            recommendations: Vec::new(),
            health_score: 0.0,
            data_skew: DataSkewMetrics {
                partition_skew_score: 0.0,
                file_size_skew_score: 0.0,
                largest_partition_size: 0,
                smallest_partition_size: 0,
                avg_partition_size: 0,
                partition_size_std_dev: 0.0,
            },
            metadata_health: MetadataHealth {
                metadata_file_count: 0,
                metadata_total_size_bytes: 0,
                avg_metadata_file_size: 0.0,
                metadata_growth_rate: 0.0,
                manifest_file_count: 0,
            },
            snapshot_health: SnapshotHealth {
                snapshot_count: 0,
                oldest_snapshot_age_days: 0.0,
                newest_snapshot_age_days: 0.0,
                avg_snapshot_age_days: 0.0,
                snapshot_retention_risk: 0.0,
            },
            deletion_vector_metrics: None,
            schema_evolution: None,
            time_travel_metrics: None,
            table_constraints: None,
            file_compaction: None,
        }
    }

    pub fn calculate_health_score(&self) -> f64 {
        let mut score = 1.0;
        
        // Penalize unreferenced files
        if self.total_files > 0 {
            let unreferenced_ratio = self.unreferenced_files.len() as f64 / self.total_files as f64;
            score -= unreferenced_ratio * 0.3;
        }
        
        // Penalize small files (inefficient)
        if self.total_files > 0 {
            let small_file_ratio = self.file_size_distribution.small_files as f64 / self.total_files as f64;
            score -= small_file_ratio * 0.2;
        }
        
        // Penalize very large files (potential performance issues)
        if self.total_files > 0 {
            let very_large_ratio = self.file_size_distribution.very_large_files as f64 / self.total_files as f64;
            score -= very_large_ratio * 0.1;
        }
        
        // Reward good partitioning
        if self.partition_count > 0 && self.total_files > 0 {
            let avg_files_per_partition = self.total_files as f64 / self.partition_count as f64;
            if avg_files_per_partition > 100.0 {
                score -= 0.1; // Too many files per partition
            } else if avg_files_per_partition < 5.0 {
                score -= 0.05; // Too few files per partition
            }
        }
        
        // Penalize data skew
        score -= self.data_skew.partition_skew_score * 0.15;
        score -= self.data_skew.file_size_skew_score * 0.1;
        
        // Penalize metadata bloat
        if self.metadata_health.metadata_total_size_bytes > 100 * 1024 * 1024 { // > 100MB
            score -= 0.05;
        }
        
        // Penalize snapshot retention issues
        score -= self.snapshot_health.snapshot_retention_risk * 0.1;
        
        // Penalize deletion vector impact
        if let Some(ref dv_metrics) = self.deletion_vector_metrics {
            score -= dv_metrics.deletion_vector_impact_score * 0.15;
        }
        
        // Factor in schema stability
        if let Some(ref schema_metrics) = self.schema_evolution {
            score -= (1.0 - schema_metrics.schema_stability_score) * 0.2;
        }
        
        // Factor in time travel storage costs
        if let Some(ref tt_metrics) = self.time_travel_metrics {
            score -= tt_metrics.storage_cost_impact_score * 0.1;
            score -= (1.0 - tt_metrics.retention_efficiency_score) * 0.05;
        }
        
        // Factor in data quality from constraints
        if let Some(ref constraint_metrics) = self.table_constraints {
            score -= (1.0 - constraint_metrics.data_quality_score) * 0.15;
            score -= constraint_metrics.constraint_violation_risk * 0.1;
        }
        
        // Factor in file compaction opportunities
        if let Some(ref compaction_metrics) = self.file_compaction {
            score -= (1.0 - compaction_metrics.compaction_opportunity_score) * 0.1;
        }
        
        score.max(0.0).min(1.0)
    }

    pub fn calculate_data_skew(&mut self) {
        if self.partitions.is_empty() {
            return;
        }

        let partition_sizes: Vec<u64> = self.partitions.iter().map(|p| p.total_size_bytes).collect();
        let file_counts: Vec<usize> = self.partitions.iter().map(|p| p.file_count).collect();

        // Calculate partition size skew
        if !partition_sizes.is_empty() {
            let total_size: u64 = partition_sizes.iter().sum();
            let avg_size = total_size as f64 / partition_sizes.len() as f64;
            
            let variance = partition_sizes.iter()
                .map(|&size| (size as f64 - avg_size).powi(2))
                .sum::<f64>() / partition_sizes.len() as f64;
            
            let std_dev = variance.sqrt();
            let coefficient_of_variation = if avg_size > 0.0 { std_dev / avg_size } else { 0.0 };
            
            self.data_skew.partition_skew_score = coefficient_of_variation.min(1.0);
            self.data_skew.largest_partition_size = *partition_sizes.iter().max().unwrap_or(&0);
            self.data_skew.smallest_partition_size = *partition_sizes.iter().min().unwrap_or(&0);
            self.data_skew.avg_partition_size = avg_size as u64;
            self.data_skew.partition_size_std_dev = std_dev;
        }

        // Calculate file count skew
        if !file_counts.is_empty() {
            let total_files: usize = file_counts.iter().sum();
            let avg_files = total_files as f64 / file_counts.len() as f64;
            
            let variance = file_counts.iter()
                .map(|&count| (count as f64 - avg_files).powi(2))
                .sum::<f64>() / file_counts.len() as f64;
            
            let std_dev = variance.sqrt();
            let coefficient_of_variation = if avg_files > 0.0 { std_dev / avg_files } else { 0.0 };
            
            self.data_skew.file_size_skew_score = coefficient_of_variation.min(1.0);
        }
    }

    pub fn calculate_metadata_health(&mut self, metadata_files: &[crate::s3_client::ObjectInfo]) {
        self.metadata_health.metadata_file_count = metadata_files.len();
        self.metadata_health.metadata_total_size_bytes = metadata_files.iter().map(|f| f.size as u64).sum();
        
        if !metadata_files.is_empty() {
            self.metadata_health.avg_metadata_file_size = 
                self.metadata_health.metadata_total_size_bytes as f64 / metadata_files.len() as f64;
        }
        
        // Estimate growth rate (simplified - would need historical data for accuracy)
        self.metadata_health.metadata_growth_rate = 0.0; // Placeholder
    }

    pub fn calculate_snapshot_health(&mut self, snapshot_count: usize) {
        self.snapshot_health.snapshot_count = snapshot_count;
        
        // Simplified snapshot age calculation (would need actual timestamps)
        self.snapshot_health.oldest_snapshot_age_days = 0.0;
        self.snapshot_health.newest_snapshot_age_days = 0.0;
        self.snapshot_health.avg_snapshot_age_days = 0.0;
        
        // Calculate retention risk based on snapshot count
        if snapshot_count > 100 {
            self.snapshot_health.snapshot_retention_risk = 0.8;
        } else if snapshot_count > 50 {
            self.snapshot_health.snapshot_retention_risk = 0.5;
        } else if snapshot_count > 20 {
            self.snapshot_health.snapshot_retention_risk = 0.2;
        } else {
            self.snapshot_health.snapshot_retention_risk = 0.0;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct DeletionVectorMetrics {
    #[pyo3(get)]
    pub deletion_vector_count: usize,
    #[pyo3(get)]
    pub total_deletion_vector_size_bytes: u64,
    #[pyo3(get)]
    pub avg_deletion_vector_size_bytes: f64,
    #[pyo3(get)]
    pub deletion_vector_age_days: f64,
    #[pyo3(get)]
    pub deleted_rows_count: u64,
    #[pyo3(get)]
    pub deletion_vector_impact_score: f64, // 0.0 = no impact, 1.0 = high impact
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct SchemaEvolutionMetrics {
    #[pyo3(get)]
    pub total_schema_changes: usize,
    #[pyo3(get)]
    pub breaking_changes: usize,
    #[pyo3(get)]
    pub non_breaking_changes: usize,
    #[pyo3(get)]
    pub schema_stability_score: f64, // 0.0 = unstable, 1.0 = very stable
    #[pyo3(get)]
    pub days_since_last_change: f64,
    #[pyo3(get)]
    pub schema_change_frequency: f64, // changes per day
    #[pyo3(get)]
    pub current_schema_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct TimeTravelMetrics {
    #[pyo3(get)]
    pub total_snapshots: usize,
    #[pyo3(get)]
    pub oldest_snapshot_age_days: f64,
    #[pyo3(get)]
    pub newest_snapshot_age_days: f64,
    #[pyo3(get)]
    pub total_historical_size_bytes: u64,
    #[pyo3(get)]
    pub avg_snapshot_size_bytes: f64,
    #[pyo3(get)]
    pub storage_cost_impact_score: f64, // 0.0 = low cost, 1.0 = high cost
    #[pyo3(get)]
    pub retention_efficiency_score: f64, // 0.0 = inefficient, 1.0 = very efficient
    #[pyo3(get)]
    pub recommended_retention_days: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct TableConstraintsMetrics {
    #[pyo3(get)]
    pub total_constraints: usize,
    #[pyo3(get)]
    pub check_constraints: usize,
    #[pyo3(get)]
    pub not_null_constraints: usize,
    #[pyo3(get)]
    pub unique_constraints: usize,
    #[pyo3(get)]
    pub foreign_key_constraints: usize,
    #[pyo3(get)]
    pub constraint_violation_risk: f64, // 0.0 = low risk, 1.0 = high risk
    #[pyo3(get)]
    pub data_quality_score: f64, // 0.0 = poor quality, 1.0 = excellent quality
    #[pyo3(get)]
    pub constraint_coverage_score: f64, // 0.0 = no coverage, 1.0 = full coverage
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct FileCompactionMetrics {
    #[pyo3(get)]
    pub compaction_opportunity_score: f64, // 0.0 = no opportunity, 1.0 = high opportunity
    #[pyo3(get)]
    pub small_files_count: usize,
    #[pyo3(get)]
    pub small_files_size_bytes: u64,
    #[pyo3(get)]
    pub potential_compaction_files: usize,
    #[pyo3(get)]
    pub estimated_compaction_savings_bytes: u64,
    #[pyo3(get)]
    pub recommended_target_file_size_bytes: u64,
    #[pyo3(get)]
    pub compaction_priority: String, // "low", "medium", "high", "critical"
    #[pyo3(get)]
    pub z_order_opportunity: bool,
    #[pyo3(get)]
    pub z_order_columns: Vec<String>,
}

impl HealthReport {
    pub fn new(table_path: String, table_type: String) -> Self {
        Self {
            table_path,
            table_type,
            analysis_timestamp: chrono::Utc::now().to_rfc3339(),
            metrics: HealthMetrics::new(),
            health_score: 0.0,
        }
    }
}
