"""Data types for Delta-Skelter analysis (pure Python dataclasses).

These dataclasses are intentionally lightweight and cover the fields
required by the Delta Lake analyzer implementation. Additional fields
can be added later as we extend functionality.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime, timezone


@dataclass
class FileInfo:
    path: str
    size_bytes: int
    last_modified: Optional[str]
    is_referenced: bool = True


@dataclass
class PartitionInfo:
    partition_values: Dict[str, str]
    file_count: int
    total_size_bytes: int
    avg_file_size_bytes: float
    files: List[FileInfo] = field(default_factory=list)


@dataclass
class FileSizeDistribution:
    small_files: int = 0
    medium_files: int = 0
    large_files: int = 0
    very_large_files: int = 0


@dataclass
class DataSkewMetrics:
    partition_skew_score: float = 0.0
    file_size_skew_score: float = 0.0
    largest_partition_size: int = 0
    smallest_partition_size: int = 0
    avg_partition_size: int = 0
    partition_size_std_dev: float = 0.0


@dataclass
class MetadataHealth:
    metadata_file_count: int = 0
    metadata_total_size_bytes: int = 0
    avg_metadata_file_size: float = 0.0
    manifest_file_count: int = 0


@dataclass
class SnapshotHealth:
    snapshot_count: int = 0
    oldest_snapshot_age_days: float = 0.0
    newest_snapshot_age_days: float = 0.0
    avg_snapshot_age_days: float = 0.0
    snapshot_retention_risk: float = 0.0


@dataclass
class HealthMetrics:
    total_files: int = 0
    total_size_bytes: int = 0
    unreferenced_files: List[FileInfo] = field(default_factory=list)
    unreferenced_size_bytes: int = 0
    partition_count: int = 0
    partitions: List[PartitionInfo] = field(default_factory=list)
    clustering: Optional[Dict] = None
    avg_file_size_bytes: float = 0.0
    file_size_distribution: FileSizeDistribution = field(
        default_factory=FileSizeDistribution
    )
    recommendations: List[str] = field(default_factory=list)
    health_score: float = 0.0
    data_skew: DataSkewMetrics = field(default_factory=DataSkewMetrics)
    metadata_health: MetadataHealth = field(default_factory=MetadataHealth)
    snapshot_health: SnapshotHealth = field(default_factory=SnapshotHealth)
    deletion_vector_metrics: Optional[Dict] = None
    schema_evolution: Optional[Dict] = None
    time_travel_metrics: Optional[Dict] = None
    table_constraints: Optional[Dict] = None
    file_compaction: Optional[Dict] = None
    table_properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class HealthReport:
    table_path: str
    table_type: str
    analysis_timestamp: str
    metrics: HealthMetrics
    health_score: float = 0.0

    @classmethod
    def new(cls, table_path: str, table_type: str) -> "HealthReport":
        return cls(
            table_path=table_path,
            table_type=table_type,
            analysis_timestamp=datetime.now(timezone.utc).isoformat(),
            metrics=HealthMetrics(),
            health_score=0.0,
        )
