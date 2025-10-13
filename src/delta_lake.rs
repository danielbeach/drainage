use crate::s3_client::S3ClientWrapper;
use crate::types::*;
use anyhow::Result;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
struct SchemaChange {
    #[allow(dead_code)]
    version: u64,
    timestamp: u64,
    schema: Value,
    is_breaking: bool,
}

pub struct DeltaLakeAnalyzer {
    s3_client: S3ClientWrapper,
}

impl DeltaLakeAnalyzer {
    pub fn new(s3_client: S3ClientWrapper) -> Self {
        Self { s3_client }
    }

    pub async fn analyze(&self) -> Result<HealthReport> {
        let mut report = HealthReport::new(
            format!(
                "s3://{}/{}",
                self.s3_client.get_bucket(),
                self.s3_client.get_prefix()
            ),
            "delta".to_string(),
        );

        // List all files in the Delta table directory
        let all_objects = self
            .s3_client
            .list_objects(self.s3_client.get_prefix())
            .await?;

        // Separate data files from metadata files
        let (data_files, metadata_files) = self.categorize_files(&all_objects)?;

        // Analyze Delta log to find referenced files
        let referenced_files = self.find_referenced_files(&metadata_files).await?;

        // Find clustering information
        let clustering_columns = self.find_clustering_info(&metadata_files).await?;

        // Calculate metrics
        let mut metrics = HealthMetrics::new();
        metrics.total_files = data_files.len();
        metrics.total_size_bytes = data_files.iter().map(|f| f.size as u64).sum();

        // Find unreferenced files
        let referenced_set: HashSet<String> = referenced_files.into_iter().collect();
        for file in &data_files {
            let file_path = format!("{}/{}", self.s3_client.get_prefix(), file.key);
            if !referenced_set.contains(&file_path) {
                metrics.unreferenced_files.push(FileInfo {
                    path: file_path,
                    size_bytes: file.size as u64,
                    last_modified: file.last_modified.clone(),
                    is_referenced: false,
                });
            }
        }

        metrics.unreferenced_size_bytes = metrics
            .unreferenced_files
            .iter()
            .map(|f| f.size_bytes)
            .sum();

        // Analyze partitioning
        self.analyze_partitioning(&data_files, &mut metrics)?;

        // Analyze clustering if clustering columns are found
        if let Some(ref clustering_cols) = clustering_columns {
            self.analyze_clustering(&data_files, clustering_cols, &mut metrics)?;
        }

        // Calculate file size distribution
        self.calculate_file_size_distribution(&data_files, &mut metrics);

        // Calculate average file size
        if metrics.total_files > 0 {
            metrics.avg_file_size_bytes =
                metrics.total_size_bytes as f64 / metrics.total_files as f64;
        }

        // Calculate additional health metrics
        metrics.calculate_data_skew();
        let metadata_files_owned: Vec<crate::s3_client::ObjectInfo> =
            metadata_files.iter().map(|f| (*f).clone()).collect();
        metrics.calculate_metadata_health(&metadata_files_owned);
        metrics.calculate_snapshot_health(metadata_files.len()); // Simplified: use metadata file count as snapshot count

        // Analyze deletion vectors
        metrics.deletion_vector_metrics = self.analyze_deletion_vectors(&metadata_files).await?;

        // Analyze schema evolution
        metrics.schema_evolution = self.analyze_schema_evolution(&metadata_files).await?;

        // Analyze time travel storage costs
        metrics.time_travel_metrics = self.analyze_time_travel(&metadata_files).await?;

        // Analyze table constraints
        metrics.table_constraints = self.analyze_table_constraints(&metadata_files).await?;

        // Analyze file compaction opportunities
        metrics.file_compaction = self
            .analyze_file_compaction(&data_files, &metadata_files)
            .await?;

        // Generate recommendations
        self.generate_recommendations(&mut metrics);

        // Calculate health score
        metrics.health_score = metrics.calculate_health_score();
        report.metrics = metrics;
        report.health_score = report.metrics.health_score;

        Ok(report)
    }

    fn categorize_files<'a>(
        &self,
        objects: &'a [crate::s3_client::ObjectInfo],
    ) -> Result<(
        Vec<&'a crate::s3_client::ObjectInfo>,
        Vec<&'a crate::s3_client::ObjectInfo>,
    )> {
        let mut data_files = Vec::new();
        let mut metadata_files = Vec::new();

        for obj in objects {
            if obj.key.ends_with(".parquet") {
                data_files.push(obj);
            } else if obj.key.contains("_delta_log/") && obj.key.ends_with(".json") {
                metadata_files.push(obj);
            }
        }

        Ok((data_files, metadata_files))
    }

    async fn find_referenced_files(
        &self,
        metadata_files: &[&crate::s3_client::ObjectInfo],
    ) -> Result<Vec<String>> {
        let mut referenced_files = Vec::new();

        for metadata_file in metadata_files {
            let content = self.s3_client.get_object(&metadata_file.key).await?;

            // Handle both single JSON objects and newline-delimited JSON (NDJSON)
            let content_str = String::from_utf8_lossy(&content);

            for line in content_str.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                // Try to parse each line as a JSON object
                match serde_json::from_str::<Value>(line) {
                    Ok(json) => {
                        if let Some(add_actions) = json.get("add") {
                            if let Some(add_array) = add_actions.as_array() {
                                for add_action in add_array {
                                    if let Some(path) = add_action.get("path") {
                                        if let Some(path_str) = path.as_str() {
                                            referenced_files.push(path_str.to_string());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // If individual line parsing fails, try parsing the entire content as a single JSON
                        if let Ok(json) = serde_json::from_slice::<Value>(&content) {
                            if let Some(add_actions) = json.get("add") {
                                if let Some(add_array) = add_actions.as_array() {
                                    for add_action in add_array {
                                        if let Some(path) = add_action.get("path") {
                                            if let Some(path_str) = path.as_str() {
                                                referenced_files.push(path_str.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        break; // Exit the line-by-line loop if we fall back to single JSON
                    }
                }
            }
        }

        Ok(referenced_files)
    }

    async fn find_clustering_info(
        &self,
        metadata_files: &[&crate::s3_client::ObjectInfo],
    ) -> Result<Option<Vec<String>>> {
        for metadata_file in metadata_files {
            let content = self.s3_client.get_object(&metadata_file.key).await?;

            // Handle both single JSON objects and newline-delimited JSON (NDJSON)
            let content_str = String::from_utf8_lossy(&content);

            for line in content_str.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                // Try to parse each line as a JSON object
                match serde_json::from_str::<Value>(line) {
                    Ok(json) => {
                        // Look for clustering information in various possible locations
                        if let Some(cluster_by) = json.get("clusterBy") {
                            if let Some(cluster_array) = cluster_by.as_array() {
                                let clustering_columns: Vec<String> = cluster_array
                                    .iter()
                                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                    .collect();
                                if !clustering_columns.is_empty() {
                                    return Ok(Some(clustering_columns));
                                }
                            }
                        }

                        // Also check for clustering in metadata section
                        if let Some(metadata) = json.get("metaData") {
                            if let Some(cluster_by) = metadata.get("clusterBy") {
                                if let Some(cluster_array) = cluster_by.as_array() {
                                    let clustering_columns: Vec<String> = cluster_array
                                        .iter()
                                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                        .collect();
                                    if !clustering_columns.is_empty() {
                                        return Ok(Some(clustering_columns));
                                    }
                                }
                            }
                        }

                        // Check for clustering in configuration
                        if let Some(configuration) = json.get("configuration") {
                            if let Some(cluster_by) = configuration.get("delta.clustering.columns")
                            {
                                if let Some(cluster_str) = cluster_by.as_str() {
                                    // Parse comma-separated clustering columns
                                    let clustering_columns: Vec<String> = cluster_str
                                        .split(',')
                                        .map(|s| s.trim().to_string())
                                        .filter(|s| !s.is_empty())
                                        .collect();
                                    if !clustering_columns.is_empty() {
                                        return Ok(Some(clustering_columns));
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // If individual line parsing fails, try parsing the entire content as a single JSON
                        if let Ok(json) = serde_json::from_slice::<Value>(&content) {
                            // Check for clustering in the full JSON
                            if let Some(cluster_by) = json.get("clusterBy") {
                                if let Some(cluster_array) = cluster_by.as_array() {
                                    let clustering_columns: Vec<String> = cluster_array
                                        .iter()
                                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                        .collect();
                                    if !clustering_columns.is_empty() {
                                        return Ok(Some(clustering_columns));
                                    }
                                }
                            }

                            if let Some(metadata) = json.get("metaData") {
                                if let Some(cluster_by) = metadata.get("clusterBy") {
                                    if let Some(cluster_array) = cluster_by.as_array() {
                                        let clustering_columns: Vec<String> = cluster_array
                                            .iter()
                                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                            .collect();
                                        if !clustering_columns.is_empty() {
                                            return Ok(Some(clustering_columns));
                                        }
                                    }
                                }
                            }
                        }
                        break; // Exit the line-by-line loop if we fall back to single JSON
                    }
                }
            }
        }

        Ok(None)
    }

    fn analyze_partitioning(
        &self,
        data_files: &[&crate::s3_client::ObjectInfo],
        metrics: &mut HealthMetrics,
    ) -> Result<()> {
        let mut partition_map: HashMap<String, PartitionInfo> = HashMap::new();

        for file in data_files {
            // Extract partition information from file path
            // Delta Lake typically uses partition columns in the path like: col1=value1/col2=value2/file.parquet
            let path_parts: Vec<&str> = file.key.split('/').collect();
            let mut partition_values = HashMap::new();
            let mut _file_name = "";

            for part in &path_parts {
                if part.contains('=') {
                    let kv: Vec<&str> = part.split('=').collect();
                    if kv.len() == 2 {
                        partition_values.insert(kv[0].to_string(), kv[1].to_string());
                    }
                } else if part.ends_with(".parquet") {
                    _file_name = part;
                }
            }

            let partition_key = serde_json::to_string(&partition_values).unwrap_or_default();

            let partition_info =
                partition_map
                    .entry(partition_key)
                    .or_insert_with(|| PartitionInfo {
                        partition_values: partition_values.clone(),
                        file_count: 0,
                        total_size_bytes: 0,
                        avg_file_size_bytes: 0.0,
                        files: Vec::new(),
                    });

            partition_info.file_count += 1;
            partition_info.total_size_bytes += file.size as u64;
            partition_info.files.push(FileInfo {
                path: format!("{}/{}", self.s3_client.get_prefix(), file.key),
                size_bytes: file.size as u64,
                last_modified: file.last_modified.clone(),
                is_referenced: true, // We'll update this later
            });
        }

        // Calculate averages for each partition
        for partition in partition_map.values_mut() {
            if partition.file_count > 0 {
                partition.avg_file_size_bytes =
                    partition.total_size_bytes as f64 / partition.file_count as f64;
            }
        }

        metrics.partitions = partition_map.into_values().collect();
        metrics.partition_count = metrics.partitions.len();

        Ok(())
    }

    fn analyze_clustering(
        &self,
        data_files: &[&crate::s3_client::ObjectInfo],
        clustering_columns: &[String],
        metrics: &mut HealthMetrics,
    ) -> Result<()> {
        if clustering_columns.is_empty() {
            return Ok(());
        }

        // For Delta Lake clustering, we analyze the distribution of files
        // Since clustering is more about data layout than explicit clusters,
        // we use partition-like analysis but call it clustering
        let total_files = data_files.len();
        let total_size = data_files.iter().map(|f| f.size as u64).sum::<u64>();

        // Calculate clustering metrics
        let cluster_count = metrics.partition_count.max(1); // Use partition count as proxy for cluster count
        let avg_files_per_cluster = if cluster_count > 0 {
            total_files as f64 / cluster_count as f64
        } else {
            0.0
        };

        let avg_cluster_size_bytes = if cluster_count > 0 {
            total_size as f64 / cluster_count as f64
        } else {
            0.0
        };

        metrics.clustering = Some(crate::types::ClusteringInfo {
            clustering_columns: clustering_columns.to_vec(),
            cluster_count,
            avg_files_per_cluster,
            avg_cluster_size_bytes,
        });

        Ok(())
    }

    fn calculate_file_size_distribution(
        &self,
        data_files: &[&crate::s3_client::ObjectInfo],
        metrics: &mut HealthMetrics,
    ) {
        for file in data_files {
            let size_mb = file.size as f64 / (1024.0 * 1024.0);

            if size_mb < 16.0 {
                metrics.file_size_distribution.small_files += 1;
            } else if size_mb < 128.0 {
                metrics.file_size_distribution.medium_files += 1;
            } else if size_mb < 1024.0 {
                metrics.file_size_distribution.large_files += 1;
            } else {
                metrics.file_size_distribution.very_large_files += 1;
            }
        }
    }

    fn generate_recommendations(&self, metrics: &mut HealthMetrics) {
        // Check for unreferenced files
        if !metrics.unreferenced_files.is_empty() {
            metrics.recommendations.push(format!(
                "Found {} unreferenced files ({} bytes). Consider cleaning up orphaned data files.",
                metrics.unreferenced_files.len(),
                metrics.unreferenced_size_bytes
            ));
        }

        // Check file size distribution
        let total_files = metrics.total_files as f64;
        if total_files > 0.0 {
            let small_file_ratio = metrics.file_size_distribution.small_files as f64 / total_files;
            if small_file_ratio > 0.5 {
                metrics.recommendations.push(
                    "High percentage of small files detected. Consider compacting to improve query performance.".to_string()
                );
            }

            let very_large_ratio =
                metrics.file_size_distribution.very_large_files as f64 / total_files;
            if very_large_ratio > 0.1 {
                metrics.recommendations.push(
                    "Some very large files detected. Consider splitting large files for better parallelism.".to_string()
                );
            }
        }

        // Check partitioning
        if metrics.partition_count > 0 {
            let avg_files_per_partition = total_files / metrics.partition_count as f64;
            if avg_files_per_partition > 100.0 {
                metrics.recommendations.push(
                    "High number of files per partition. Consider repartitioning to reduce file count.".to_string()
                );
            } else if avg_files_per_partition < 5.0 {
                metrics.recommendations.push(
                    "Low number of files per partition. Consider consolidating partitions."
                        .to_string(),
                );
            }
        }

        // Check for empty partitions
        let empty_partitions = metrics
            .partitions
            .iter()
            .filter(|p| p.file_count == 0)
            .count();
        if empty_partitions > 0 {
            metrics.recommendations.push(format!(
                "Found {} empty partitions. Consider removing empty partition directories.",
                empty_partitions
            ));
        }

        // Check data skew
        if metrics.data_skew.partition_skew_score > 0.5 {
            metrics.recommendations.push(
                "High partition skew detected. Consider repartitioning to balance data distribution.".to_string()
            );
        }

        if metrics.data_skew.file_size_skew_score > 0.5 {
            metrics.recommendations.push(
                "High file size skew detected. Consider running OPTIMIZE to balance file sizes."
                    .to_string(),
            );
        }

        // Check metadata health
        if metrics.metadata_health.metadata_total_size_bytes > 50 * 1024 * 1024 {
            // > 50MB
            metrics.recommendations.push(
                "Large metadata size detected. Consider running VACUUM to clean up old transaction logs.".to_string()
            );
        }

        // Check snapshot health
        if metrics.snapshot_health.snapshot_retention_risk > 0.7 {
            metrics.recommendations.push(
                "High snapshot retention risk. Consider running VACUUM to remove old snapshots."
                    .to_string(),
            );
        }

        // Check clustering
        if let Some(ref clustering) = metrics.clustering {
            if clustering.avg_files_per_cluster > 50.0 {
                metrics.recommendations.push(
                    "High number of files per cluster. Consider optimizing clustering strategy."
                        .to_string(),
                );
            }

            if clustering.clustering_columns.len() > 4 {
                metrics.recommendations.push(
                    "Too many clustering columns detected. Consider reducing to 4 or fewer columns for optimal performance.".to_string()
                );
            }

            if clustering.clustering_columns.is_empty() {
                metrics.recommendations.push(
                    "No clustering detected. Consider enabling liquid clustering for better query performance.".to_string()
                );
            }
        }

        // Check deletion vectors
        if let Some(ref dv_metrics) = metrics.deletion_vector_metrics {
            if dv_metrics.deletion_vector_impact_score > 0.7 {
                metrics.recommendations.push(
                    "High deletion vector impact detected. Consider running VACUUM to clean up old deletion vectors.".to_string()
                );
            }

            if dv_metrics.deletion_vector_count > 50 {
                metrics.recommendations.push(
                    "Many deletion vectors detected. Consider optimizing delete operations to reduce fragmentation.".to_string()
                );
            }

            if dv_metrics.deletion_vector_age_days > 30.0 {
                metrics.recommendations.push(
                    "Old deletion vectors detected. Consider running VACUUM to clean up deletion vectors older than 30 days.".to_string()
                );
            }
        }

        // Check schema evolution
        if let Some(ref schema_metrics) = metrics.schema_evolution {
            if schema_metrics.schema_stability_score < 0.5 {
                metrics.recommendations.push(
                    "Unstable schema detected. Consider planning schema changes more carefully to improve performance.".to_string()
                );
            }

            if schema_metrics.breaking_changes > 5 {
                metrics.recommendations.push(
                    "Many breaking schema changes detected. Consider using schema evolution features to avoid breaking changes.".to_string()
                );
            }

            if schema_metrics.schema_change_frequency > 1.0 {
                metrics.recommendations.push(
                    "High schema change frequency detected. Consider batching schema changes to reduce performance impact.".to_string()
                );
            }

            if schema_metrics.days_since_last_change < 1.0 {
                metrics.recommendations.push(
                    "Recent schema changes detected. Monitor query performance for potential issues.".to_string()
                );
            }
        }

        // Check time travel storage costs
        if let Some(ref tt_metrics) = metrics.time_travel_metrics {
            if tt_metrics.storage_cost_impact_score > 0.7 {
                metrics.recommendations.push(
                    "High time travel storage costs detected. Consider running VACUUM to clean up old snapshots.".to_string()
                );
            }

            if tt_metrics.retention_efficiency_score < 0.5 {
                metrics.recommendations.push(
                    "Inefficient snapshot retention detected. Consider optimizing retention policy.".to_string()
                );
            }

            if tt_metrics.total_snapshots > 1000 {
                metrics.recommendations.push(
                    "High snapshot count detected. Consider reducing retention period to improve performance.".to_string()
                );
            }
        }

        // Check table constraints
        if let Some(ref constraint_metrics) = metrics.table_constraints {
            if constraint_metrics.data_quality_score < 0.5 {
                metrics.recommendations.push(
                    "Low data quality score detected. Consider adding more table constraints."
                        .to_string(),
                );
            }

            if constraint_metrics.constraint_violation_risk > 0.7 {
                metrics.recommendations.push(
                    "High constraint violation risk detected. Monitor data quality and consider data validation.".to_string()
                );
            }

            if constraint_metrics.constraint_coverage_score < 0.3 {
                metrics.recommendations.push(
                    "Low constraint coverage detected. Consider adding check constraints for better data quality.".to_string()
                );
            }
        }

        // Check file compaction opportunities
        if let Some(ref compaction_metrics) = metrics.file_compaction {
            if compaction_metrics.compaction_opportunity_score > 0.7 {
                metrics.recommendations.push(
                    "High file compaction opportunity detected. Consider running OPTIMIZE to improve performance.".to_string()
                );
            }

            if compaction_metrics.compaction_priority == "critical" {
                metrics.recommendations.push(
                    "Critical compaction priority detected. Run OPTIMIZE immediately to improve query performance.".to_string()
                );
            }

            if compaction_metrics.z_order_opportunity {
                metrics.recommendations.push(
                    format!("Z-ordering opportunity detected. Consider running OPTIMIZE ZORDER BY ({}) to improve query performance.", 
                            compaction_metrics.z_order_columns.join(", ")).to_string()
                );
            }

            if compaction_metrics.estimated_compaction_savings_bytes > 100 * 1024 * 1024 {
                // > 100MB
                let savings_mb = compaction_metrics.estimated_compaction_savings_bytes as f64
                    / (1024.0 * 1024.0);
                metrics.recommendations.push(
                    format!("Significant compaction savings available: {:.1} MB. Consider running OPTIMIZE.", savings_mb).to_string()
                );
            }
        }
    }

    async fn analyze_schema_evolution(
        &self,
        metadata_files: &[&crate::s3_client::ObjectInfo],
    ) -> Result<Option<crate::types::SchemaEvolutionMetrics>> {
        let mut schema_changes = Vec::new();
        let mut current_version = 0;

        // Sort metadata files by version number
        let mut sorted_files = metadata_files.to_vec();
        sorted_files.sort_by_key(|f| {
            f.key
                .split('/')
                .next_back()
                .and_then(|name| name.split('.').next())
                .and_then(|version| version.parse::<u64>().ok())
                .unwrap_or(0)
        });

        for metadata_file in &sorted_files {
            let content = self.s3_client.get_object(&metadata_file.key).await?;
            let content_str = String::from_utf8_lossy(&content);

            for line in content_str.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                match serde_json::from_str::<Value>(line) {
                    Ok(json) => {
                        // Check for schema changes in metadata
                        if let Some(metadata) = json.get("metaData") {
                            if let Some(schema_string) = metadata.get("schemaString") {
                                if let Ok(schema) = serde_json::from_str::<Value>(
                                    schema_string.as_str().unwrap_or(""),
                                ) {
                                    let is_breaking =
                                        self.is_breaking_change(&schema_changes, &schema);
                                    schema_changes.push(SchemaChange {
                                        version: current_version,
                                        timestamp: json
                                            .get("timestamp")
                                            .and_then(|t| t.as_u64())
                                            .unwrap_or(0),
                                        schema,
                                        is_breaking,
                                    });
                                }
                            }
                        }

                        // Check for protocol changes (breaking)
                        if let Some(protocol) = json.get("protocol") {
                            if let Some(reader_version) = protocol.get("minReaderVersion") {
                                let new_version = reader_version.as_u64().unwrap_or(0);
                                if new_version > current_version {
                                    schema_changes.push(SchemaChange {
                                        version: current_version,
                                        timestamp: json
                                            .get("timestamp")
                                            .and_then(|t| t.as_u64())
                                            .unwrap_or(0),
                                        schema: Value::Null,
                                        is_breaking: true,
                                    });
                                    current_version = new_version;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Try parsing the entire content as a single JSON
                        if let Ok(json) = serde_json::from_slice::<Value>(&content) {
                            if let Some(metadata) = json.get("metaData") {
                                if let Some(schema_string) = metadata.get("schemaString") {
                                    if let Ok(schema) = serde_json::from_str::<Value>(
                                        schema_string.as_str().unwrap_or(""),
                                    ) {
                                        let is_breaking =
                                            self.is_breaking_change(&schema_changes, &schema);
                                        schema_changes.push(SchemaChange {
                                            version: current_version,
                                            timestamp: json
                                                .get("timestamp")
                                                .and_then(|t| t.as_u64())
                                                .unwrap_or(0),
                                            schema,
                                            is_breaking,
                                        });
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }
            current_version += 1;
        }

        if schema_changes.is_empty() {
            return Ok(None);
        }

        self.calculate_schema_metrics(schema_changes, current_version)
    }

    fn is_breaking_change(&self, previous_changes: &[SchemaChange], new_schema: &Value) -> bool {
        if previous_changes.is_empty() {
            return false;
        }

        let last_schema = &previous_changes.last().unwrap().schema;

        // Check for breaking changes:
        // 1. Column removal
        // 2. Column type changes
        // 3. Required field changes
        self.detect_breaking_schema_changes(last_schema, new_schema)
    }

    fn detect_breaking_schema_changes(&self, old_schema: &Value, new_schema: &Value) -> bool {
        // Simplified breaking change detection
        // In a real implementation, this would be more sophisticated
        if let (Some(old_fields), Some(new_fields)) =
            (old_schema.get("fields"), new_schema.get("fields"))
        {
            if let (Some(old_fields_array), Some(new_fields_array)) =
                (old_fields.as_array(), new_fields.as_array())
            {
                // Check if any fields were removed
                let old_field_names: HashSet<String> = old_fields_array
                    .iter()
                    .filter_map(|f| {
                        f.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                    })
                    .collect();
                let new_field_names: HashSet<String> = new_fields_array
                    .iter()
                    .filter_map(|f| {
                        f.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                    })
                    .collect();

                // If any old fields are missing, it's a breaking change
                if !old_field_names.is_subset(&new_field_names) {
                    return true;
                }

                // Check for type changes in existing fields
                for old_field in old_fields_array {
                    if let Some(field_name) = old_field.get("name").and_then(|n| n.as_str()) {
                        if let Some(new_field) = new_fields_array
                            .iter()
                            .find(|f| f.get("name").and_then(|n| n.as_str()) == Some(field_name))
                        {
                            let old_type = old_field.get("type").and_then(|t| t.as_str());
                            let new_type = new_field.get("type").and_then(|t| t.as_str());

                            // If types changed, it's a breaking change
                            if old_type != new_type {
                                return true;
                            }

                            // Check if nullable changed from false to true (breaking)
                            let old_nullable = old_field
                                .get("nullable")
                                .and_then(|n| n.as_bool())
                                .unwrap_or(true);
                            let new_nullable = new_field
                                .get("nullable")
                                .and_then(|n| n.as_bool())
                                .unwrap_or(true);

                            if !old_nullable && new_nullable {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        false
    }

    fn calculate_schema_metrics(
        &self,
        changes: Vec<SchemaChange>,
        current_version: u64,
    ) -> Result<Option<crate::types::SchemaEvolutionMetrics>> {
        let total_changes = changes.len();
        let breaking_changes = changes.iter().filter(|c| c.is_breaking).count();
        let non_breaking_changes = total_changes - breaking_changes;

        // Calculate time-based metrics
        let now = chrono::Utc::now().timestamp() as u64;
        let days_since_last = if let Some(last_change) = changes.last() {
            (now - last_change.timestamp / 1000) as f64 / 86400.0
        } else {
            365.0 // No changes in a year = very stable
        };

        // Calculate change frequency (changes per day)
        let total_days = if changes.len() > 1 {
            let first_change = changes.first().unwrap().timestamp / 1000;
            let last_change = changes.last().unwrap().timestamp / 1000;
            ((last_change - first_change) as f64 / 86400.0).max(1.0_f64)
        } else {
            1.0
        };

        let change_frequency = total_changes as f64 / total_days;

        // Calculate stability score
        let stability_score = self.calculate_schema_stability_score(
            total_changes,
            breaking_changes,
            change_frequency,
            days_since_last,
        );

        Ok(Some(crate::types::SchemaEvolutionMetrics {
            total_schema_changes: total_changes,
            breaking_changes,
            non_breaking_changes,
            schema_stability_score: stability_score,
            days_since_last_change: days_since_last,
            schema_change_frequency: change_frequency,
            current_schema_version: current_version,
        }))
    }

    fn calculate_schema_stability_score(
        &self,
        total_changes: usize,
        breaking_changes: usize,
        frequency: f64,
        days_since_last: f64,
    ) -> f64 {
        let mut score: f64 = 1.0;

        // Penalize total changes
        if total_changes > 50 {
            score -= 0.3;
        } else if total_changes > 20 {
            score -= 0.2;
        } else if total_changes > 10 {
            score -= 0.1;
        }

        // Penalize breaking changes heavily
        if breaking_changes > 10 {
            score -= 0.4;
        } else if breaking_changes > 5 {
            score -= 0.3;
        } else if breaking_changes > 0 {
            score -= 0.2;
        }

        // Penalize high frequency changes
        if frequency > 1.0 {
            // More than 1 change per day
            score -= 0.3;
        } else if frequency > 0.5 {
            // More than 1 change every 2 days
            score -= 0.2;
        } else if frequency > 0.1 {
            // More than 1 change every 10 days
            score -= 0.1;
        }

        // Reward stability (no recent changes)
        if days_since_last > 30.0 {
            score += 0.1;
        } else if days_since_last > 7.0 {
            score += 0.05;
        }

        score.clamp(0.0_f64, 1.0_f64)
    }

    async fn analyze_deletion_vectors(
        &self,
        metadata_files: &[&crate::s3_client::ObjectInfo],
    ) -> Result<Option<crate::types::DeletionVectorMetrics>> {
        let mut deletion_vector_count = 0;
        let mut total_size = 0;
        let mut deleted_rows = 0;
        let mut oldest_dv_age: f64 = 0.0;

        for metadata_file in metadata_files {
            let content = self.s3_client.get_object(&metadata_file.key).await?;
            let content_str = String::from_utf8_lossy(&content);

            for line in content_str.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                match serde_json::from_str::<Value>(line) {
                    Ok(json) => {
                        // Look for remove actions (deletions)
                        if let Some(remove_actions) = json.get("remove") {
                            if let Some(remove_array) = remove_actions.as_array() {
                                for remove_action in remove_array {
                                    // Check if deletion vector is used
                                    if let Some(deletion_vector) =
                                        remove_action.get("deletionVector")
                                    {
                                        deletion_vector_count += 1;

                                        // Parse deletion vector size
                                        if let Some(size) = deletion_vector.get("sizeInBytes") {
                                            total_size += size.as_u64().unwrap_or(0);
                                        }

                                        // Parse deleted rows count
                                        if let Some(rows) = deletion_vector.get("cardinality") {
                                            deleted_rows += rows.as_u64().unwrap_or(0);
                                        }

                                        // Parse creation time for age calculation
                                        if let Some(timestamp) = remove_action.get("timestamp") {
                                            let creation_time =
                                                timestamp.as_u64().unwrap_or(0) as i64;
                                            let age_days = (chrono::Utc::now().timestamp()
                                                - creation_time / 1000)
                                                as f64
                                                / 86400.0;
                                            oldest_dv_age = oldest_dv_age.max(age_days);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Try parsing the entire content as a single JSON
                        if let Ok(json) = serde_json::from_slice::<Value>(&content) {
                            if let Some(remove_actions) = json.get("remove") {
                                if let Some(remove_array) = remove_actions.as_array() {
                                    for remove_action in remove_array {
                                        if let Some(deletion_vector) =
                                            remove_action.get("deletionVector")
                                        {
                                            deletion_vector_count += 1;

                                            if let Some(size) = deletion_vector.get("sizeInBytes") {
                                                total_size += size.as_u64().unwrap_or(0);
                                            }

                                            if let Some(rows) = deletion_vector.get("cardinality") {
                                                deleted_rows += rows.as_u64().unwrap_or(0);
                                            }

                                            if let Some(timestamp) = remove_action.get("timestamp")
                                            {
                                                let creation_time =
                                                    timestamp.as_u64().unwrap_or(0) as i64;
                                                let age_days = (chrono::Utc::now().timestamp()
                                                    - creation_time / 1000)
                                                    as f64
                                                    / 86400.0;
                                                oldest_dv_age = oldest_dv_age.max(age_days);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }

        if deletion_vector_count == 0 {
            return Ok(None);
        }

        let avg_size = total_size as f64 / deletion_vector_count as f64;
        let impact_score =
            self.calculate_deletion_vector_impact(deletion_vector_count, total_size, oldest_dv_age);

        Ok(Some(crate::types::DeletionVectorMetrics {
            deletion_vector_count,
            total_deletion_vector_size_bytes: total_size,
            avg_deletion_vector_size_bytes: avg_size,
            deletion_vector_age_days: oldest_dv_age,
            deleted_rows_count: deleted_rows,
            deletion_vector_impact_score: impact_score,
        }))
    }

    fn calculate_deletion_vector_impact(&self, count: usize, size: u64, age: f64) -> f64 {
        let mut impact: f64 = 0.0;

        // Impact from count (more DVs = higher impact)
        if count > 100 {
            impact += 0.3;
        } else if count > 50 {
            impact += 0.2;
        } else if count > 10 {
            impact += 0.1;
        }

        // Impact from size (larger DVs = higher impact)
        let size_mb = size as f64 / (1024.0 * 1024.0);
        if size_mb > 100.0 {
            impact += 0.3;
        } else if size_mb > 50.0 {
            impact += 0.2;
        } else if size_mb > 10.0 {
            impact += 0.1;
        }

        // Impact from age (older DVs = higher impact)
        if age > 30.0 {
            impact += 0.4;
        } else if age > 7.0 {
            impact += 0.2;
        }

        impact.min(1.0_f64)
    }

    async fn analyze_time_travel(
        &self,
        metadata_files: &[&crate::s3_client::ObjectInfo],
    ) -> Result<Option<crate::types::TimeTravelMetrics>> {
        let mut total_snapshots = 0;
        let mut total_historical_size = 0u64;
        let mut oldest_timestamp = chrono::Utc::now().timestamp() as u64;
        let mut newest_timestamp = 0u64;

        // Analyze all metadata files to understand time travel storage
        for metadata_file in metadata_files {
            let content = self.s3_client.get_object(&metadata_file.key).await?;
            let content_str = String::from_utf8_lossy(&content);

            for line in content_str.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                match serde_json::from_str::<Value>(line) {
                    Ok(json) => {
                        if let Some(timestamp) = json.get("timestamp") {
                            let ts = timestamp.as_u64().unwrap_or(0);
                            if ts > 0 {
                                total_snapshots += 1;
                                oldest_timestamp = oldest_timestamp.min(ts);
                                newest_timestamp = newest_timestamp.max(ts);

                                // Estimate snapshot size based on actions
                                let snapshot_size = self.estimate_snapshot_size(&json);
                                total_historical_size += snapshot_size;
                            }
                        }
                    }
                    Err(_) => {
                        // Try parsing the entire content as a single JSON
                        if let Ok(json) = serde_json::from_slice::<Value>(&content) {
                            if let Some(timestamp) = json.get("timestamp") {
                                let ts = timestamp.as_u64().unwrap_or(0);
                                if ts > 0 {
                                    total_snapshots += 1;
                                    oldest_timestamp = oldest_timestamp.min(ts);
                                    newest_timestamp = newest_timestamp.max(ts);

                                    let snapshot_size = self.estimate_snapshot_size(&json);
                                    total_historical_size += snapshot_size;
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }

        if total_snapshots == 0 {
            return Ok(None);
        }

        let now = chrono::Utc::now().timestamp() as u64;
        let oldest_age_days = (now - oldest_timestamp / 1000) as f64 / 86400.0;
        let newest_age_days = (now - newest_timestamp / 1000) as f64 / 86400.0;
        let avg_snapshot_size = total_historical_size as f64 / total_snapshots as f64;

        let storage_cost_impact = self.calculate_storage_cost_impact(
            total_historical_size,
            total_snapshots,
            oldest_age_days,
        );
        let retention_efficiency =
            self.calculate_retention_efficiency(total_snapshots, oldest_age_days, newest_age_days);
        let recommended_retention =
            self.calculate_recommended_retention(total_snapshots, oldest_age_days);

        Ok(Some(crate::types::TimeTravelMetrics {
            total_snapshots,
            oldest_snapshot_age_days: oldest_age_days,
            newest_snapshot_age_days: newest_age_days,
            total_historical_size_bytes: total_historical_size,
            avg_snapshot_size_bytes: avg_snapshot_size,
            storage_cost_impact_score: storage_cost_impact,
            retention_efficiency_score: retention_efficiency,
            recommended_retention_days: recommended_retention,
        }))
    }

    fn estimate_snapshot_size(&self, json: &Value) -> u64 {
        let mut size = 0u64;

        // Estimate size based on actions in the transaction log
        if let Some(add_actions) = json.get("add") {
            if let Some(add_array) = add_actions.as_array() {
                for add_action in add_array {
                    if let Some(file_size) = add_action.get("sizeInBytes") {
                        size += file_size.as_u64().unwrap_or(0);
                    }
                }
            }
        }

        // Add metadata overhead (estimated)
        size + 1024 // 1KB overhead per snapshot
    }

    fn calculate_storage_cost_impact(
        &self,
        total_size: u64,
        snapshot_count: usize,
        oldest_age: f64,
    ) -> f64 {
        let mut impact: f64 = 0.0;

        // Impact from total size
        let size_gb = total_size as f64 / (1024.0 * 1024.0 * 1024.0);
        if size_gb > 100.0 {
            impact += 0.4;
        } else if size_gb > 50.0 {
            impact += 0.3;
        } else if size_gb > 10.0 {
            impact += 0.2;
        } else if size_gb > 1.0 {
            impact += 0.1;
        }

        // Impact from snapshot count
        if snapshot_count > 1000 {
            impact += 0.3;
        } else if snapshot_count > 500 {
            impact += 0.2;
        } else if snapshot_count > 100 {
            impact += 0.1;
        }

        // Impact from age (older snapshots = higher cost)
        if oldest_age > 365.0 {
            impact += 0.3;
        } else if oldest_age > 90.0 {
            impact += 0.2;
        } else if oldest_age > 30.0 {
            impact += 0.1;
        }

        impact.min(1.0_f64)
    }

    fn calculate_retention_efficiency(
        &self,
        snapshot_count: usize,
        oldest_age: f64,
        newest_age: f64,
    ) -> f64 {
        let mut efficiency: f64 = 1.0;

        // Penalize too many snapshots
        if snapshot_count > 1000 {
            efficiency -= 0.4;
        } else if snapshot_count > 500 {
            efficiency -= 0.3;
        } else if snapshot_count > 100 {
            efficiency -= 0.2;
        } else if snapshot_count > 50 {
            efficiency -= 0.1;
        }

        // Reward appropriate retention period
        let retention_days = oldest_age - newest_age;
        if retention_days > 365.0 {
            efficiency -= 0.2; // Too long retention
        } else if retention_days < 7.0 {
            efficiency -= 0.1; // Too short retention
        }

        efficiency.clamp(0.0_f64, 1.0_f64)
    }

    fn calculate_recommended_retention(&self, snapshot_count: usize, oldest_age: f64) -> u64 {
        // Simple heuristic: recommend retention based on snapshot count and age
        if snapshot_count > 1000 || oldest_age > 365.0 {
            30 // 30 days for high snapshot count or very old data
        } else if snapshot_count > 500 || oldest_age > 90.0 {
            60 // 60 days for medium snapshot count or old data
        } else if snapshot_count > 100 || oldest_age > 30.0 {
            90 // 90 days for moderate snapshot count or recent data
        } else {
            180 // 180 days for low snapshot count and recent data
        }
    }

    async fn analyze_table_constraints(
        &self,
        metadata_files: &[&crate::s3_client::ObjectInfo],
    ) -> Result<Option<crate::types::TableConstraintsMetrics>> {
        let mut total_constraints = 0;
        let mut check_constraints = 0;
        let mut not_null_constraints = 0;
        let mut unique_constraints = 0;
        let mut foreign_key_constraints = 0;

        // Analyze metadata files for constraint information
        for metadata_file in metadata_files {
            let content = self.s3_client.get_object(&metadata_file.key).await?;
            let content_str = String::from_utf8_lossy(&content);

            for line in content_str.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                match serde_json::from_str::<Value>(line) {
                    Ok(json) => {
                        if let Some(metadata) = json.get("metaData") {
                            if let Some(schema_string) = metadata.get("schemaString") {
                                if let Ok(schema) = serde_json::from_str::<Value>(
                                    schema_string.as_str().unwrap_or(""),
                                ) {
                                    let constraints = self.extract_constraints_from_schema(&schema);
                                    total_constraints += constraints.0;
                                    check_constraints += constraints.1;
                                    not_null_constraints += constraints.2;
                                    unique_constraints += constraints.3;
                                    foreign_key_constraints += constraints.4;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Try parsing the entire content as a single JSON
                        if let Ok(json) = serde_json::from_slice::<Value>(&content) {
                            if let Some(metadata) = json.get("metaData") {
                                if let Some(schema_string) = metadata.get("schemaString") {
                                    if let Ok(schema) = serde_json::from_str::<Value>(
                                        schema_string.as_str().unwrap_or(""),
                                    ) {
                                        let constraints =
                                            self.extract_constraints_from_schema(&schema);
                                        total_constraints += constraints.0;
                                        check_constraints += constraints.1;
                                        not_null_constraints += constraints.2;
                                        unique_constraints += constraints.3;
                                        foreign_key_constraints += constraints.4;
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }

        if total_constraints == 0 {
            return Ok(None);
        }

        let constraint_violation_risk =
            self.calculate_constraint_violation_risk(total_constraints, check_constraints);
        let data_quality_score =
            self.calculate_data_quality_score(total_constraints, constraint_violation_risk);
        let constraint_coverage_score =
            self.calculate_constraint_coverage_score(total_constraints, check_constraints);

        Ok(Some(crate::types::TableConstraintsMetrics {
            total_constraints,
            check_constraints,
            not_null_constraints,
            unique_constraints,
            foreign_key_constraints,
            constraint_violation_risk,
            data_quality_score,
            constraint_coverage_score,
        }))
    }

    fn extract_constraints_from_schema(
        &self,
        schema: &Value,
    ) -> (usize, usize, usize, usize, usize) {
        let mut total = 0;
        let mut check = 0;
        let mut not_null = 0;
        let mut unique = 0;
        let mut foreign_key = 0;

        if let Some(fields) = schema.get("fields") {
            if let Some(fields_array) = fields.as_array() {
                for field in fields_array {
                    total += 1;

                    // Check for NOT NULL constraint
                    if let Some(nullable) = field.get("nullable") {
                        if !nullable.as_bool().unwrap_or(true) {
                            not_null += 1;
                        }
                    }

                    // Check for other constraints (simplified)
                    if let Some(metadata) = field.get("metadata") {
                        if let Some(metadata_obj) = metadata.as_object() {
                            for (key, _) in metadata_obj {
                                if key.contains("constraint") || key.contains("check") {
                                    check += 1;
                                }
                                if key.contains("unique") {
                                    unique += 1;
                                }
                                if key.contains("foreign") || key.contains("reference") {
                                    foreign_key += 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        (total, check, not_null, unique, foreign_key)
    }

    fn calculate_constraint_violation_risk(
        &self,
        total_constraints: usize,
        check_constraints: usize,
    ) -> f64 {
        if total_constraints == 0 {
            return 0.0;
        }

        // Higher risk with more complex constraints
        let complexity_ratio = check_constraints as f64 / total_constraints as f64;
        if complexity_ratio > 0.5 {
            0.8
        } else if complexity_ratio > 0.3 {
            0.6
        } else if complexity_ratio > 0.1 {
            0.4
        } else {
            0.2
        }
    }

    fn calculate_data_quality_score(&self, total_constraints: usize, violation_risk: f64) -> f64 {
        let mut score = 1.0;

        // Reward having constraints
        if total_constraints > 10 {
            score += 0.2;
        } else if total_constraints > 5 {
            score += 0.1;
        }

        // Penalize violation risk
        score -= violation_risk * 0.5;

        score.clamp(0.0_f64, 1.0_f64)
    }

    fn calculate_constraint_coverage_score(
        &self,
        total_constraints: usize,
        check_constraints: usize,
    ) -> f64 {
        if total_constraints == 0 {
            return 0.0;
        }

        let coverage_ratio = check_constraints as f64 / total_constraints as f64;
        if coverage_ratio > 0.5 {
            1.0
        } else if coverage_ratio > 0.3 {
            0.7
        } else if coverage_ratio > 0.1 {
            0.4
        } else {
            0.1
        }
    }

    async fn analyze_file_compaction(
        &self,
        data_files: &[&crate::s3_client::ObjectInfo],
        metadata_files: &[&crate::s3_client::ObjectInfo],
    ) -> Result<Option<crate::types::FileCompactionMetrics>> {
        let mut small_files_count = 0;
        let mut small_files_size = 0u64;
        let mut potential_compaction_files = 0;
        let mut estimated_savings = 0u64;

        // Analyze file sizes for compaction opportunities
        for file in data_files {
            let file_size = file.size as u64;
            if file_size < 16 * 1024 * 1024 {
                // < 16MB
                small_files_count += 1;
                small_files_size += file_size;
                potential_compaction_files += 1;
            }
        }

        // Calculate potential savings
        if small_files_count > 1 {
            let target_size = 128 * 1024 * 1024; // 128MB target
            let files_per_target = (target_size as f64
                / (small_files_size as f64 / small_files_count as f64))
                .ceil() as usize;
            let target_files = (small_files_count as f64 / files_per_target as f64).ceil() as usize;
            let estimated_target_size = target_files as u64 * target_size / 2; // Conservative estimate
            estimated_savings = small_files_size.saturating_sub(estimated_target_size);
        }

        let compaction_opportunity = self.calculate_compaction_opportunity(
            small_files_count,
            small_files_size,
            data_files.len(),
        );
        let recommended_target_size = self.calculate_recommended_target_size(data_files);
        let compaction_priority =
            self.calculate_compaction_priority(compaction_opportunity, small_files_count);
        let (z_order_opportunity, z_order_columns) =
            self.analyze_z_order_opportunity(metadata_files).await?;

        Ok(Some(crate::types::FileCompactionMetrics {
            compaction_opportunity_score: compaction_opportunity,
            small_files_count,
            small_files_size_bytes: small_files_size,
            potential_compaction_files,
            estimated_compaction_savings_bytes: estimated_savings,
            recommended_target_file_size_bytes: recommended_target_size,
            compaction_priority,
            z_order_opportunity,
            z_order_columns,
        }))
    }

    fn calculate_compaction_opportunity(
        &self,
        small_files: usize,
        small_files_size: u64,
        total_files: usize,
    ) -> f64 {
        if total_files == 0 {
            return 0.0;
        }

        let small_file_ratio = small_files as f64 / total_files as f64;
        let _size_ratio = small_files_size as f64 / (small_files_size as f64 + 1.0); // Avoid division by zero

        if small_file_ratio > 0.8 {
            1.0
        } else if small_file_ratio > 0.6 {
            0.8
        } else if small_file_ratio > 0.4 {
            0.6
        } else if small_file_ratio > 0.2 {
            0.4
        } else {
            0.2
        }
    }

    fn calculate_recommended_target_size(
        &self,
        data_files: &[&crate::s3_client::ObjectInfo],
    ) -> u64 {
        if data_files.is_empty() {
            return 128 * 1024 * 1024; // 128MB default
        }

        let total_size = data_files.iter().map(|f| f.size as u64).sum::<u64>();
        let avg_size = total_size as f64 / data_files.len() as f64;

        // Recommend target size based on current average
        if avg_size < 16.0 * 1024.0 * 1024.0 {
            128 * 1024 * 1024 // 128MB for small files
        } else if avg_size < 64.0 * 1024.0 * 1024.0 {
            256 * 1024 * 1024 // 256MB for medium files
        } else {
            512 * 1024 * 1024 // 512MB for large files
        }
    }

    fn calculate_compaction_priority(&self, opportunity_score: f64, small_files: usize) -> String {
        if opportunity_score > 0.8 || small_files > 100 {
            "critical".to_string()
        } else if opportunity_score > 0.6 || small_files > 50 {
            "high".to_string()
        } else if opportunity_score > 0.4 || small_files > 20 {
            "medium".to_string()
        } else {
            "low".to_string()
        }
    }

    async fn analyze_z_order_opportunity(
        &self,
        metadata_files: &[&crate::s3_client::ObjectInfo],
    ) -> Result<(bool, Vec<String>)> {
        // Look for clustering columns that could benefit from Z-ordering
        for metadata_file in metadata_files {
            let content = self.s3_client.get_object(&metadata_file.key).await?;
            let content_str = String::from_utf8_lossy(&content);

            for line in content_str.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                match serde_json::from_str::<Value>(line) {
                    Ok(json) => {
                        // Look for clustering information
                        if let Some(cluster_by) = json.get("clusterBy") {
                            if let Some(cluster_array) = cluster_by.as_array() {
                                let clustering_columns: Vec<String> = cluster_array
                                    .iter()
                                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                    .collect();
                                if !clustering_columns.is_empty() {
                                    return Ok((true, clustering_columns));
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        Ok((false, Vec::new()))
    }
}
