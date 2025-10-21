use pyo3::prelude::*;

mod delta_lake;
mod health_analyzer;
mod iceberg;
mod storage_client;
mod types;

use health_analyzer::HealthAnalyzer;

/// A Python module implemented in Rust for analyzing data lake health
#[pymodule]
fn drainage(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(analyze_delta_lake, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_iceberg, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_table, m)?)?;
    m.add_function(wrap_pyfunction!(print_health_report, m)?)?;
    Ok(())
}

/// Analyze Delta Lake table health
#[pyfunction]
fn analyze_delta_lake(
    storage_path: String,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    aws_region: Option<String>,
    gcs_service_account_key: Option<String>,
) -> PyResult<types::HealthReport> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let analyzer = HealthAnalyzer::create_async(
            storage_path,
            aws_access_key_id,
            aws_secret_access_key,
            aws_region,
            gcs_service_account_key,
        )
        .await?;
        analyzer.analyze_delta_lake().await
    })
}

/// Analyze Apache Iceberg table health
#[pyfunction]
fn analyze_iceberg(
    storage_path: String,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    aws_region: Option<String>,
    gcs_service_account_key: Option<String>,
) -> PyResult<types::HealthReport> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let analyzer = HealthAnalyzer::create_async(
            storage_path,
            aws_access_key_id,
            aws_secret_access_key,
            aws_region,
            gcs_service_account_key,
        )
        .await?;
        analyzer.analyze_iceberg().await
    })
}

/// Analyze table health with automatic table type detection
#[pyfunction]
fn analyze_table(
    storage_path: String,
    table_type: Option<String>,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    aws_region: Option<String>,
    gcs_service_account_key: Option<String>,
) -> PyResult<types::HealthReport> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let analyzer = HealthAnalyzer::create_async(storage_path.clone(), aws_access_key_id, aws_secret_access_key, aws_region, gcs_service_account_key).await?;
        // If table type is specified, use it directly
        if let Some(ref ttype) = table_type {
            match ttype.to_lowercase().as_str() {
                "delta" | "delta_lake" => analyzer.analyze_delta_lake().await,
                "iceberg" | "apache_iceberg" => analyzer.analyze_iceberg().await,
                _ => Err(pyo3::exceptions::PyValueError::new_err(
                    format!("Unknown table type: {}. Supported types: 'delta', 'iceberg'", ttype)
                )),
            }
        } else {
            // Auto-detect table type by checking for characteristic files
            let objects = analyzer.list_objects_for_detection().await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to list objects: {}", e)))?;
            // Check for Delta Lake characteristic files
            let has_delta_log = objects.iter().any(|obj| obj.key.contains("_delta_log/") && obj.key.ends_with(".json"));
            // Check for Iceberg characteristic files
            let has_iceberg_metadata = objects.iter().any(|obj| obj.key.ends_with("metadata.json"));
            if has_delta_log && !has_iceberg_metadata {
                analyzer.analyze_delta_lake().await
            } else if has_iceberg_metadata && !has_delta_log {
                analyzer.analyze_iceberg().await
            } else if has_delta_log && has_iceberg_metadata {
                Err(pyo3::exceptions::PyValueError::new_err(
                    "Ambiguous table type: both Delta Lake and Iceberg files detected. Please specify table_type explicitly."
                ))
            } else {
                Err(pyo3::exceptions::PyValueError::new_err(
                    "Could not determine table type. No Delta Lake (_delta_log) or Iceberg (metadata.json) files found. Please specify table_type explicitly."
                ))
            }
        }
    })
}

/// Print a comprehensive health report with nice formatting
#[pyfunction]
fn print_health_report(report: &types::HealthReport) -> PyResult<()> {
    // Print header
    println!("\n{}", "=".repeat(60));
    println!("Table Health Report: {}", report.table_path);
    println!("Type: {}", report.table_type);
    println!("Analysis Time: {}", report.analysis_timestamp);
    println!("{}\n", "=".repeat(60));

    // Overall health score
    let health_emoji = if report.health_score > 0.8 {
        "🟢"
    } else if report.health_score > 0.6 {
        "🟡"
    } else {
        "🔴"
    };
    println!(
        "{} Overall Health Score: {:.1}%",
        health_emoji,
        report.health_score * 100.0
    );

    // Key metrics
    println!("\n📊 Key Metrics:");
    println!("{}", "─".repeat(60));
    println!("  Total Files:         {}", report.metrics.total_files);

    // Format size in GB or MB
    let size_gb = report.metrics.total_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    if size_gb >= 1.0 {
        println!("  Total Size:          {:.2} GB", size_gb);
    } else {
        let size_mb = report.metrics.total_size_bytes as f64 / (1024.0 * 1024.0);
        println!("  Total Size:          {:.2} MB", size_mb);
    }

    // Average file size
    let avg_mb = report.metrics.avg_file_size_bytes / (1024.0 * 1024.0);
    println!("  Average File Size:   {:.2} MB", avg_mb);
    println!("  Partition Count:     {}", report.metrics.partition_count);

    // File size distribution
    println!("\n📦 File Size Distribution:");
    println!("{}", "─".repeat(60));
    let dist = &report.metrics.file_size_distribution;
    let total_files =
        (dist.small_files + dist.medium_files + dist.large_files + dist.very_large_files) as f64;

    if total_files > 0.0 {
        println!(
            "  Small (<16MB):       {:>6} files ({:>5.1}%)",
            dist.small_files,
            dist.small_files as f64 / total_files * 100.0
        );
        println!(
            "  Medium (16-128MB):   {:>6} files ({:>5.1}%)",
            dist.medium_files,
            dist.medium_files as f64 / total_files * 100.0
        );
        println!(
            "  Large (128MB-1GB):   {:>6} files ({:>5.1}%)",
            dist.large_files,
            dist.large_files as f64 / total_files * 100.0
        );
        println!(
            "  Very Large (>1GB):   {:>6} files ({:>5.1}%)",
            dist.very_large_files,
            dist.very_large_files as f64 / total_files * 100.0
        );
    }

    // Clustering information (Iceberg only)
    if let Some(ref clustering) = report.metrics.clustering {
        println!("\n🎯 Clustering Information:");
        println!("{}", "─".repeat(60));
        println!(
            "  Clustering Columns:  {}",
            clustering.clustering_columns.join(", ")
        );
        println!("  Cluster Count:       {}", clustering.cluster_count);
        println!(
            "  Avg Files/Cluster:   {:.2}",
            clustering.avg_files_per_cluster
        );
        let cluster_size_mb = clustering.avg_cluster_size_bytes / (1024.0 * 1024.0);
        println!("  Avg Cluster Size:    {:.2} MB", cluster_size_mb);
    }

    // Data skew analysis
    println!("\n📊 Data Skew Analysis:");
    println!("{}", "─".repeat(60));
    let skew = &report.metrics.data_skew;
    println!(
        "  Partition Skew Score: {:.2} (0=perfect, 1=highly skewed)",
        skew.partition_skew_score
    );
    println!(
        "  File Size Skew:       {:.2} (0=perfect, 1=highly skewed)",
        skew.file_size_skew_score
    );
    if skew.avg_partition_size > 0 {
        let largest_mb = skew.largest_partition_size as f64 / (1024.0 * 1024.0);
        let smallest_mb = skew.smallest_partition_size as f64 / (1024.0 * 1024.0);
        let avg_mb = skew.avg_partition_size as f64 / (1024.0 * 1024.0);
        println!("  Largest Partition:   {:.2} MB", largest_mb);
        println!("  Smallest Partition:  {:.2} MB", smallest_mb);
        println!("  Avg Partition Size:  {:.2} MB", avg_mb);
    }

    // Metadata health
    println!("\n📋 Metadata Health:");
    println!("{}", "─".repeat(60));
    let meta = &report.metrics.metadata_health;
    println!("  Metadata Files:       {}", meta.metadata_file_count);
    let meta_size_mb = meta.metadata_total_size_bytes as f64 / (1024.0 * 1024.0);
    println!("  Metadata Size:        {:.2} MB", meta_size_mb);
    if meta.metadata_file_count > 0 {
        println!(
            "  Avg Metadata File:    {:.2} MB",
            meta.avg_metadata_file_size / (1024.0 * 1024.0)
        );
    }
    if meta.manifest_file_count > 0 {
        println!("  Manifest Files:       {}", meta.manifest_file_count);
    }

    // Snapshot health
    println!("\n📸 Snapshot Health:");
    println!("{}", "─".repeat(60));
    let snap = &report.metrics.snapshot_health;
    println!("  Snapshot Count:       {}", snap.snapshot_count);
    println!(
        "  Retention Risk:       {:.1}%",
        snap.snapshot_retention_risk * 100.0
    );
    if snap.oldest_snapshot_age_days > 0.0 {
        println!(
            "  Oldest Snapshot:      {:.1} days",
            snap.oldest_snapshot_age_days
        );
        println!(
            "  Newest Snapshot:      {:.1} days",
            snap.newest_snapshot_age_days
        );
        println!(
            "  Avg Snapshot Age:     {:.1} days",
            snap.avg_snapshot_age_days
        );
    }

    // Unreferenced files warning
    if !report.metrics.unreferenced_files.is_empty() {
        println!("\n⚠️  Unreferenced Files:");
        println!("{}", "─".repeat(60));
        println!("  Count:  {}", report.metrics.unreferenced_files.len());
        let wasted_gb = report.metrics.unreferenced_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        if wasted_gb >= 1.0 {
            println!("  Wasted: {:.2} GB", wasted_gb);
        } else {
            let wasted_mb = report.metrics.unreferenced_size_bytes as f64 / (1024.0 * 1024.0);
            println!("  Wasted: {:.2} MB", wasted_mb);
        }

        let table_type_name = if report.table_type == "delta" {
            "Delta transaction log"
        } else {
            "Iceberg manifest files"
        };
        println!("\n  These files exist in S3 but are not referenced in the");
        println!("  {}. Consider cleaning them up.", table_type_name);
    }

    // Deletion vector metrics (Delta Lake only)
    if let Some(ref dv_metrics) = report.metrics.deletion_vector_metrics {
        println!("\n🗑️  Deletion Vector Analysis:");
        println!("{}", "─".repeat(60));
        println!(
            "  Deletion Vectors:      {}",
            dv_metrics.deletion_vector_count
        );
        let dv_size_mb = dv_metrics.total_deletion_vector_size_bytes as f64 / (1024.0 * 1024.0);
        if dv_size_mb >= 1.0 {
            println!("  Total DV Size:         {:.2} MB", dv_size_mb);
        } else {
            let dv_size_kb = dv_metrics.total_deletion_vector_size_bytes as f64 / 1024.0;
            println!("  Total DV Size:         {:.2} KB", dv_size_kb);
        }
        println!("  Deleted Rows:          {}", dv_metrics.deleted_rows_count);
        println!(
            "  Oldest DV Age:         {:.1} days",
            dv_metrics.deletion_vector_age_days
        );
        println!(
            "  Impact Score:          {:.2} (0=no impact, 1=high impact)",
            dv_metrics.deletion_vector_impact_score
        );
    }

    // Schema evolution metrics
    if let Some(ref schema_metrics) = report.metrics.schema_evolution {
        println!("\n📋 Schema Evolution Analysis:");
        println!("{}", "─".repeat(60));
        println!(
            "  Total Changes:         {}",
            schema_metrics.total_schema_changes
        );
        println!(
            "  Breaking Changes:      {}",
            schema_metrics.breaking_changes
        );
        println!(
            "  Non-Breaking Changes:  {}",
            schema_metrics.non_breaking_changes
        );
        println!(
            "  Stability Score:       {:.2} (0=unstable, 1=very stable)",
            schema_metrics.schema_stability_score
        );
        println!(
            "  Days Since Last:       {:.1} days",
            schema_metrics.days_since_last_change
        );
        println!(
            "  Change Frequency:      {:.3} changes/day",
            schema_metrics.schema_change_frequency
        );
        println!(
            "  Current Version:       {}",
            schema_metrics.current_schema_version
        );
    }

    // Time travel analysis
    if let Some(ref tt_metrics) = report.metrics.time_travel_metrics {
        println!("\n⏰ Time Travel Analysis:");
        println!("{}", "─".repeat(60));
        println!("  Total Snapshots:       {}", tt_metrics.total_snapshots);
        println!(
            "  Oldest Snapshot:       {:.1} days",
            tt_metrics.oldest_snapshot_age_days
        );
        println!(
            "  Newest Snapshot:       {:.1} days",
            tt_metrics.newest_snapshot_age_days
        );
        let historical_gb =
            tt_metrics.total_historical_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        if historical_gb >= 1.0 {
            println!("  Historical Size:       {:.2} GB", historical_gb);
        } else {
            let historical_mb = tt_metrics.total_historical_size_bytes as f64 / (1024.0 * 1024.0);
            println!("  Historical Size:       {:.2} MB", historical_mb);
        }
        println!(
            "  Storage Cost Impact:   {:.2} (0=low cost, 1=high cost)",
            tt_metrics.storage_cost_impact_score
        );
        println!(
            "  Retention Efficiency:  {:.2} (0=inefficient, 1=very efficient)",
            tt_metrics.retention_efficiency_score
        );
        println!(
            "  Recommended Retention: {} days",
            tt_metrics.recommended_retention_days
        );
    }

    // Table constraints analysis
    if let Some(ref constraint_metrics) = report.metrics.table_constraints {
        println!("\n🔒 Table Constraints Analysis:");
        println!("{}", "─".repeat(60));
        println!(
            "  Total Constraints:     {}",
            constraint_metrics.total_constraints
        );
        println!(
            "  Check Constraints:     {}",
            constraint_metrics.check_constraints
        );
        println!(
            "  NOT NULL Constraints:  {}",
            constraint_metrics.not_null_constraints
        );
        println!(
            "  Unique Constraints:    {}",
            constraint_metrics.unique_constraints
        );
        println!(
            "  Foreign Key Constraints: {}",
            constraint_metrics.foreign_key_constraints
        );
        println!(
            "  Violation Risk:        {:.2} (0=low risk, 1=high risk)",
            constraint_metrics.constraint_violation_risk
        );
        println!(
            "  Data Quality Score:    {:.2} (0=poor quality, 1=excellent quality)",
            constraint_metrics.data_quality_score
        );
        println!(
            "  Constraint Coverage:   {:.2} (0=no coverage, 1=full coverage)",
            constraint_metrics.constraint_coverage_score
        );
    }

    // File compaction analysis
    if let Some(ref compaction_metrics) = report.metrics.file_compaction {
        println!("\n📦 File Compaction Analysis:");
        println!("{}", "─".repeat(60));
        println!(
            "  Compaction Opportunity: {:.2} (0=no opportunity, 1=high opportunity)",
            compaction_metrics.compaction_opportunity_score
        );
        println!(
            "  Small Files Count:     {}",
            compaction_metrics.small_files_count
        );
        let small_files_mb = compaction_metrics.small_files_size_bytes as f64 / (1024.0 * 1024.0);
        println!("  Small Files Size:      {:.2} MB", small_files_mb);
        println!(
            "  Potential Compaction:  {} files",
            compaction_metrics.potential_compaction_files
        );
        let savings_mb =
            compaction_metrics.estimated_compaction_savings_bytes as f64 / (1024.0 * 1024.0);
        if savings_mb >= 1.0 {
            println!("  Estimated Savings:     {:.2} MB", savings_mb);
        } else {
            let savings_kb = compaction_metrics.estimated_compaction_savings_bytes as f64 / 1024.0;
            println!("  Estimated Savings:     {:.2} KB", savings_kb);
        }
        let target_mb =
            compaction_metrics.recommended_target_file_size_bytes as f64 / (1024.0 * 1024.0);
        println!("  Recommended Target:    {:.0} MB", target_mb);
        println!(
            "  Compaction Priority:   {}",
            compaction_metrics.compaction_priority.to_uppercase()
        );
        println!(
            "  Z-Order Opportunity:   {}",
            if compaction_metrics.z_order_opportunity {
                "Yes"
            } else {
                "No"
            }
        );
        if !compaction_metrics.z_order_columns.is_empty() {
            println!(
                "  Z-Order Columns:       {}",
                compaction_metrics.z_order_columns.join(", ")
            );
        }
    }

    // Recommendations
    if !report.metrics.recommendations.is_empty() {
        println!("\n💡 Recommendations:");
        println!("{}", "─".repeat(60));
        for (i, rec) in report.metrics.recommendations.iter().enumerate() {
            println!("  {}. {}", i + 1, rec);
        }
    } else {
        println!("\n✅ No recommendations - table is in excellent health!");
    }

    println!("\n{}\n", "=".repeat(60));

    Ok(())
}
