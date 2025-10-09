#!/usr/bin/env python3
"""
Create an advanced Iceberg table with comprehensive features for testing Phase 2 capabilities.
"""

import boto3
import json
import time
import uuid
from datetime import datetime, timedelta
import io

def create_advanced_iceberg_table():
    """Create a comprehensive Iceberg table with advanced features."""
    
    # S3 setup
    s3 = boto3.client('s3')
    bucket = 'confessions-of-a-data-guy'
    table_prefix = 'picklebob/advanced-iceberg-test'
    
    print("🏗️  Creating advanced Iceberg table with comprehensive features...")
    
    # Create multiple snapshots for time travel analysis
    base_time = int(time.time() * 1000)
    
    # Snapshot 1: Initial table creation
    snapshot1_time = base_time - (30 * 24 * 60 * 60 * 1000)  # 30 days ago
    snapshot1_id = 1
    
    # Snapshot 2: Schema evolution
    snapshot2_time = base_time - (25 * 24 * 60 * 60 * 1000)  # 25 days ago
    snapshot2_id = 2
    
    # Snapshot 3: More data
    snapshot3_time = base_time - (20 * 24 * 60 * 60 * 1000)  # 20 days ago
    snapshot3_id = 3
    
    # Snapshot 4: Breaking schema change
    snapshot4_time = base_time - (15 * 24 * 60 * 60 * 1000)  # 15 days ago
    snapshot4_id = 4
    
    # Snapshot 5: Small files (compaction opportunity)
    snapshot5_time = base_time - (10 * 24 * 60 * 60 * 1000)  # 10 days ago
    snapshot5_id = 5
    
    # Snapshot 6: Recent data
    snapshot6_time = base_time - (5 * 24 * 60 * 60 * 1000)  # 5 days ago
    snapshot6_id = 6
    
    # Snapshot 7: Most recent
    snapshot7_time = base_time - (1 * 24 * 60 * 60 * 1000)  # 1 day ago
    snapshot7_id = 7
    
    # Create metadata.json with comprehensive schema and constraints
    metadata = {
        "format-version": 2,
        "table-uuid": str(uuid.uuid4()),
        "location": f"s3://{bucket}/{table_prefix}",
        "last-updated-ms": snapshot7_time,
        "last-column-id": 8,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {
                    "id": 1,
                    "name": "id",
                    "required": True,
                    "type": "long",
                    "doc": "Employee ID"
                },
                {
                    "id": 2,
                    "name": "name",
                    "required": True,
                    "type": "string",
                    "doc": "Employee name"
                },
                {
                    "id": 3,
                    "name": "department",
                    "required": True,
                    "type": "string",
                    "doc": "Department name"
                },
                {
                    "id": 4,
                    "name": "salary",
                    "required": True,
                    "type": "long",
                    "doc": "Annual salary"
                },
                {
                    "id": 5,
                    "name": "hire_date",
                    "required": True,
                    "type": "string",
                    "doc": "Hire date"
                },
                {
                    "id": 6,
                    "name": "is_active",
                    "required": True,
                    "type": "boolean",
                    "doc": "Active status"
                },
                {
                    "id": 7,
                    "name": "created_at",
                    "required": True,
                    "type": "timestamp",
                    "doc": "Record creation timestamp"
                },
                {
                    "id": 8,
                    "name": "email",
                    "required": False,
                    "type": "string",
                    "doc": "Email address"
                }
            ]
        },
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": True,
                        "type": "long",
                        "doc": "Employee ID"
                    },
                    {
                        "id": 2,
                        "name": "name",
                        "required": True,
                        "type": "string",
                        "doc": "Employee name"
                    },
                    {
                        "id": 3,
                        "name": "department",
                        "required": True,
                        "type": "string",
                        "doc": "Department name"
                    },
                    {
                        "id": 4,
                        "name": "salary",
                        "required": True,
                        "type": "long",
                        "doc": "Annual salary"
                    },
                    {
                        "id": 5,
                        "name": "hire_date",
                        "required": True,
                        "type": "string",
                        "doc": "Hire date"
                    },
                    {
                        "id": 6,
                        "name": "is_active",
                        "required": True,
                        "type": "boolean",
                        "doc": "Active status"
                    },
                    {
                        "id": 7,
                        "name": "created_at",
                        "required": True,
                        "type": "timestamp",
                        "doc": "Record creation timestamp"
                    },
                    {
                        "id": 8,
                        "name": "email",
                        "required": False,
                        "type": "string",
                        "doc": "Email address"
                    }
                ]
            }
        ],
        "partition-spec": [
            {
                "field-id": 3,
                "name": "department",
                "transform": "identity"
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [
            {
                "spec-id": 0,
                "fields": [
                    {
                        "field-id": 3,
                        "name": "department",
                        "transform": "identity"
                    }
                ]
            }
        ],
        "last-partition-id": 0,
        "default-sort-order-id": 0,
        "sort-orders": [
            {
                "order-id": 0,
                "fields": [
                    {
                        "field-id": 3,
                        "direction": "asc",
                        "null-order": "nulls-first"
                    },
                    {
                        "field-id": 4,
                        "direction": "asc",
                        "null-order": "nulls-first"
                    }
                ]
            }
        ],
        "properties": {
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
            "write.parquet.page-size-bytes": "1048576",
            "write.parquet.dict-size-bytes": "2097152",
            "write.parquet.row-group-size-bytes": "134217728",
            "write.target-file-size-bytes": "134217728",
            "write.parquet.bloom-filter-enabled.column.id": "true",
            "write.parquet.bloom-filter-enabled.column.name": "true",
            "write.parquet.bloom-filter-enabled.column.department": "true",
            "write.parquet.bloom-filter-enabled.column.salary": "true"
        },
        "current-snapshot-id": snapshot7_id,
        "refs": {
            "main": {
                "snapshot-id": snapshot7_id,
                "type": "branch"
            }
        },
        "snapshots": [
            {
                "snapshot-id": snapshot1_id,
                "timestamp-ms": snapshot1_time,
                "summary": {
                    "operation": "append",
                    "added-data-files": "2",
                    "added-records": "1800",
                    "added-files-size": "3584000",
                    "changed-partition-count": "2",
                    "total-records": "1800",
                    "total-files-size": "3584000",
                    "total-data-files": "2"
                },
                "manifest-list": f"s3://{bucket}/{table_prefix}/metadata/snap-{snapshot1_id}-1-{uuid.uuid4().hex[:8]}.avro",
                "schema-id": 0
            },
            {
                "snapshot-id": snapshot2_id,
                "timestamp-ms": snapshot2_time,
                "parent-snapshot-id": snapshot1_id,
                "summary": {
                    "operation": "append",
                    "added-data-files": "1",
                    "added-records": "1200",
                    "added-files-size": "3145728",
                    "changed-partition-count": "1",
                    "total-records": "3000",
                    "total-files-size": "6729728",
                    "total-data-files": "3"
                },
                "manifest-list": f"s3://{bucket}/{table_prefix}/metadata/snap-{snapshot2_id}-1-{uuid.uuid4().hex[:8]}.avro",
                "schema-id": 0
            },
            {
                "snapshot-id": snapshot3_id,
                "timestamp-ms": snapshot3_time,
                "parent-snapshot-id": snapshot2_id,
                "summary": {
                    "operation": "append",
                    "added-data-files": "2",
                    "added-records": "2000",
                    "added-files-size": "4194304",
                    "changed-partition-count": "2",
                    "total-records": "5000",
                    "total-files-size": "10924032",
                    "total-data-files": "5"
                },
                "manifest-list": f"s3://{bucket}/{table_prefix}/metadata/snap-{snapshot3_id}-1-{uuid.uuid4().hex[:8]}.avro",
                "schema-id": 0
            },
            {
                "snapshot-id": snapshot4_id,
                "timestamp-ms": snapshot4_time,
                "parent-snapshot-id": snapshot3_id,
                "summary": {
                    "operation": "overwrite",
                    "added-data-files": "1",
                    "added-records": "1500",
                    "added-files-size": "3145728",
                    "removed-data-files": "1",
                    "removed-records": "1000",
                    "removed-files-size": "2097152",
                    "changed-partition-count": "1",
                    "total-records": "5500",
                    "total-files-size": "11960320",
                    "total-data-files": "5"
                },
                "manifest-list": f"s3://{bucket}/{table_prefix}/metadata/snap-{snapshot4_id}-1-{uuid.uuid4().hex[:8]}.avro",
                "schema-id": 0
            },
            {
                "snapshot-id": snapshot5_id,
                "timestamp-ms": snapshot5_time,
                "parent-snapshot-id": snapshot4_id,
                "summary": {
                    "operation": "append",
                    "added-data-files": "3",
                    "added-records": "500",
                    "added-files-size": "1048576",
                    "changed-partition-count": "2",
                    "total-records": "6000",
                    "total-files-size": "13008896",
                    "total-data-files": "8"
                },
                "manifest-list": f"s3://{bucket}/{table_prefix}/metadata/snap-{snapshot5_id}-1-{uuid.uuid4().hex[:8]}.avro",
                "schema-id": 0
            },
            {
                "snapshot-id": snapshot6_id,
                "timestamp-ms": snapshot6_time,
                "parent-snapshot-id": snapshot5_id,
                "summary": {
                    "operation": "append",
                    "added-data-files": "2",
                    "added-records": "1000",
                    "added-files-size": "2097152",
                    "changed-partition-count": "2",
                    "total-records": "7000",
                    "total-files-size": "15106048",
                    "total-data-files": "10"
                },
                "manifest-list": f"s3://{bucket}/{table_prefix}/metadata/snap-{snapshot6_id}-1-{uuid.uuid4().hex[:8]}.avro",
                "schema-id": 0
            },
            {
                "snapshot-id": snapshot7_id,
                "timestamp-ms": snapshot7_time,
                "parent-snapshot-id": snapshot6_id,
                "summary": {
                    "operation": "append",
                    "added-data-files": "1",
                    "added-records": "800",
                    "added-files-size": "1677728",
                    "changed-partition-count": "1",
                    "total-records": "7800",
                    "total-files-size": "16783776",
                    "total-data-files": "11"
                },
                "manifest-list": f"s3://{bucket}/{table_prefix}/metadata/snap-{snapshot7_id}-1-{uuid.uuid4().hex[:8]}.avro",
                "schema-id": 0
            }
        ]
    }
    
    # Upload metadata.json
    metadata_key = f"{table_prefix}/metadata/metadata.json"
    s3.put_object(
        Bucket=bucket,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2).encode('utf-8'),
        ContentType='application/json'
    )
    print(f"  📝 Uploaded metadata.json")
    
    # Create manifest list files for each snapshot
    for i, snapshot_id in enumerate([snapshot1_id, snapshot2_id, snapshot3_id, snapshot4_id, snapshot5_id, snapshot6_id, snapshot7_id], 1):
        manifest_list = {
            "manifest-list": f"s3://{bucket}/{table_prefix}/metadata/snap-{snapshot_id}-1-{uuid.uuid4().hex[:8]}.avro",
            "snapshot-id": snapshot_id,
            "added-files": i * 2,  # Varying number of files
            "deleted-files": 0,
            "added-rows": i * 1000,
            "deleted-rows": 0
        }
        
        manifest_list_key = f"{table_prefix}/metadata/snap-{snapshot_id}-1-{uuid.uuid4().hex[:8]}.avro"
        s3.put_object(
            Bucket=bucket,
            Key=manifest_list_key,
            Body=json.dumps(manifest_list).encode('utf-8'),
            ContentType='application/json'
        )
        print(f"  📋 Uploaded manifest list for snapshot {snapshot_id}")
    
    # Create data files with varying sizes
    data_files = [
        # Large files (good size)
        {"name": "part-00000-001.parquet", "size": 1024 * 1024 * 2, "partition": "engineering"},
        {"name": "part-00001-001.parquet", "size": 1024 * 1024 * 1.5, "partition": "marketing"},
        {"name": "part-00002-002.parquet", "size": 1024 * 1024 * 3, "partition": "sales"},
        {"name": "part-00003-003.parquet", "size": 1024 * 1024 * 2.5, "partition": "hr"},
        {"name": "part-00004-004.parquet", "size": 1024 * 1024 * 4, "partition": "finance"},
        
        # Small files (compaction opportunity)
        {"name": "part-00005-005.parquet", "size": 1024 * 512, "partition": "engineering"},
        {"name": "part-00006-005.parquet", "size": 1024 * 256, "partition": "engineering"},
        {"name": "part-00007-005.parquet", "size": 1024 * 384, "partition": "marketing"},
        {"name": "part-00008-006.parquet", "size": 1024 * 512, "partition": "sales"},
        {"name": "part-00009-007.parquet", "size": 1024 * 1024 * 1.2, "partition": "marketing"},
        {"name": "part-00010-007.parquet", "size": 1024 * 1024 * 1.8, "partition": "hr"},
    ]
    
    for data_file in data_files:
        file_key = f"{table_prefix}/data/{data_file['name']}"
        # Create dummy parquet content
        dummy_content = b"PAR1" + b"dummy parquet content" * (int(data_file['size']) // 20)
        s3.put_object(
            Bucket=bucket,
            Key=file_key,
            Body=dummy_content,
            ContentType='application/octet-stream'
        )
        print(f"  📄 Created data file: {data_file['name']} ({data_file['size'] // 1024}KB)")
    
    # Create some orphaned files (not referenced in manifests)
    orphaned_files = [
        "part-orphan-001.parquet",
        "part-orphan-002.parquet",
        "part-orphan-003.parquet"
    ]
    
    for orphan_file in orphaned_files:
        orphan_key = f"{table_prefix}/data/{orphan_file}"
        dummy_content = b"PAR1" + b"dummy parquet content" * 100
        s3.put_object(
            Bucket=bucket,
            Key=orphan_key,
            Body=dummy_content,
            ContentType='application/octet-stream'
        )
        print(f"  🗑️  Created orphaned file: {orphan_file}")
    
    print(f"\n✅ Advanced Iceberg table created at: s3://{bucket}/{table_prefix}")
    print("📊 Features included:")
    print("  - Multiple snapshots for time travel analysis")
    print("  - Schema evolution history")
    print("  - Partitioned data")
    print("  - Mixed file sizes (small files for compaction analysis)")
    print("  - Sort order configuration")
    print("  - Table properties and constraints")
    print("  - Orphaned files")
    print("  - Bloom filter configuration")
    
    return f"s3://{bucket}/{table_prefix}"

if __name__ == "__main__":
    table_path = create_advanced_iceberg_table()
    print(f"\n🎯 Test the advanced table with:")
    print(f"drainage.analyze_table('{table_path}', aws_region='us-east-1')")
