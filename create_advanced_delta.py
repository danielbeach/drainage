#!/usr/bin/env python3
"""
Create an advanced Delta Lake table with comprehensive features for testing Phase 2 capabilities.
"""

import boto3
import json
import time
import uuid
from datetime import datetime, timedelta
import io

def create_advanced_delta_table():
    """Create a comprehensive Delta Lake table with advanced features."""
    
    # S3 setup
    s3 = boto3.client('s3')
    bucket = 'confessions-of-a-data-guy'
    table_prefix = 'picklebob/advanced-delta-test'
    
    print("🏗️  Creating advanced Delta Lake table with comprehensive features...")
    
    # Create multiple transaction logs to simulate time travel
    base_time = int(time.time() * 1000)
    
    # Transaction 1: Initial table creation with schema
    txn1_time = base_time - (30 * 24 * 60 * 60 * 1000)  # 30 days ago
    txn1 = {
        "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
        "metaData": {
            "id": str(uuid.uuid4()),
            "name": "advanced_test_table",
            "description": "Advanced test table with comprehensive features",
            "format": {"provider": "parquet"},
            "schemaString": json.dumps({
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "long", "nullable": False, "metadata": {}},
                    {"name": "name", "type": "string", "nullable": False, "metadata": {}},
                    {"name": "department", "type": "string", "nullable": False, "metadata": {}},
                    {"name": "salary", "type": "double", "nullable": False, "metadata": {}},
                    {"name": "hire_date", "type": "string", "nullable": False, "metadata": {}},
                    {"name": "is_active", "type": "boolean", "nullable": False, "metadata": {}},
                    {"name": "created_at", "type": "timestamp", "nullable": False, "metadata": {}}
                ]
            }),
            "partitionColumns": ["department"],
            "configuration": {
                "delta.autoOptimize.autoCompact": "true",
                "delta.autoOptimize.optimizeWrite": "true"
            },
            "createdTime": txn1_time
        },
        "add": [
            {
                "path": "part-00000-001.parquet",
                "partitionValues": {"department": "engineering"},
                "size": 1024 * 1024 * 2,  # 2MB
                "modificationTime": txn1_time,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 1000,
                    "minValues": {"id": 1, "salary": 50000},
                    "maxValues": {"id": 1000, "salary": 150000},
                    "nullCount": {"id": 0, "salary": 0}
                })
            },
            {
                "path": "part-00001-001.parquet", 
                "partitionValues": {"department": "marketing"},
                "size": 1024 * 1024 * 1.5,  # 1.5MB
                "modificationTime": txn1_time,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 800,
                    "minValues": {"id": 1001, "salary": 45000},
                    "maxValues": {"id": 1800, "salary": 120000},
                    "nullCount": {"id": 0, "salary": 0}
                })
            }
        ],
        "commitInfo": {
            "timestamp": txn1_time,
            "operation": "CREATE TABLE",
            "operationParameters": {},
            "isolationLevel": "WriteSerializable",
            "isBlindAppend": False
        }
    }
    
    # Transaction 2: Schema evolution (non-breaking)
    txn2_time = base_time - (25 * 24 * 60 * 60 * 1000)  # 25 days ago
    txn2 = {
        "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
        "metaData": {
            "id": str(uuid.uuid4()),
            "name": "advanced_test_table",
            "description": "Advanced test table with comprehensive features",
            "format": {"provider": "parquet"},
            "schemaString": json.dumps({
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "long", "nullable": False, "metadata": {}},
                    {"name": "name", "type": "string", "nullable": False, "metadata": {}},
                    {"name": "department", "type": "string", "nullable": False, "metadata": {}},
                    {"name": "salary", "type": "double", "nullable": False, "metadata": {}},
                    {"name": "hire_date", "type": "string", "nullable": False, "metadata": {}},
                    {"name": "is_active", "type": "boolean", "nullable": False, "metadata": {}},
                    {"name": "created_at", "type": "timestamp", "nullable": False, "metadata": {}},
                    {"name": "email", "type": "string", "nullable": True, "metadata": {}}  # Added field
                ]
            }),
            "partitionColumns": ["department"],
            "configuration": {
                "delta.autoOptimize.autoCompact": "true",
                "delta.autoOptimize.optimizeWrite": "true"
            },
            "createdTime": txn1_time
        },
        "add": [
            {
                "path": "part-00002-002.parquet",
                "partitionValues": {"department": "sales"},
                "size": 1024 * 1024 * 3,  # 3MB
                "modificationTime": txn2_time,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 1200,
                    "minValues": {"id": 2001, "salary": 40000},
                    "maxValues": {"id": 3200, "salary": 200000},
                    "nullCount": {"id": 0, "salary": 0, "email": 200}
                })
            }
        ],
        "commitInfo": {
            "timestamp": txn2_time,
            "operation": "WRITE",
            "operationParameters": {},
            "isolationLevel": "WriteSerializable",
            "isBlindAppend": False
        }
    }
    
    # Transaction 3: Deletions with deletion vectors
    txn3_time = base_time - (20 * 24 * 60 * 60 * 1000)  # 20 days ago
    txn3 = {
        "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
        "remove": [
            {
                "path": "part-00000-001.parquet",
                "deletionVector": {
                    "storageType": "u",
                    "pathOrInlineData": "dummy-deletion-vector-data-1",
                    "sizeInBytes": 1024,
                    "cardinality": 50
                },
                "dataChange": True,
                "extendedFileMetadata": True,
                "partitionValues": {"department": "engineering"},
                "size": 1024 * 1024 * 2,
                "modificationTime": txn1_time
            }
        ],
        "add": [
            {
                "path": "part-00003-003.parquet",
                "partitionValues": {"department": "engineering"},
                "size": 1024 * 1024 * 1.8,  # 1.8MB
                "modificationTime": txn3_time,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 950,
                    "minValues": {"id": 1, "salary": 50000},
                    "maxValues": {"id": 1000, "salary": 150000},
                    "nullCount": {"id": 0, "salary": 0}
                })
            }
        ],
        "commitInfo": {
            "timestamp": txn3_time,
            "operation": "DELETE",
            "operationParameters": {"predicate": "id < 50"},
            "isolationLevel": "WriteSerializable",
            "isBlindAppend": False
        }
    }
    
    # Transaction 4: More data with clustering
    txn4_time = base_time - (15 * 24 * 60 * 60 * 1000)  # 15 days ago
    txn4 = {
        "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
        "clusterBy": ["department", "salary"],  # Liquid clustering
        "add": [
            {
                "path": "part-00004-004.parquet",
                "partitionValues": {"department": "hr"},
                "size": 1024 * 1024 * 2.5,  # 2.5MB
                "modificationTime": txn4_time,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 1100,
                    "minValues": {"id": 4001, "salary": 35000},
                    "maxValues": {"id": 5100, "salary": 100000},
                    "nullCount": {"id": 0, "salary": 0}
                })
            }
        ],
        "commitInfo": {
            "timestamp": txn4_time,
            "operation": "WRITE",
            "operationParameters": {},
            "isolationLevel": "WriteSerializable",
            "isBlindAppend": False
        }
    }
    
    # Transaction 5: Breaking schema change
    txn5_time = base_time - (10 * 24 * 60 * 60 * 1000)  # 10 days ago
    txn5 = {
        "protocol": {"minReaderVersion": 2, "minWriterVersion": 3},  # Breaking change
        "metaData": {
            "id": str(uuid.uuid4()),
            "name": "advanced_test_table",
            "description": "Advanced test table with comprehensive features",
            "format": {"provider": "parquet"},
            "schemaString": json.dumps({
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "long", "nullable": False, "metadata": {}},
                    {"name": "name", "type": "string", "nullable": False, "metadata": {}},
                    {"name": "department", "type": "string", "nullable": False, "metadata": {}},
                    {"name": "salary", "type": "long", "nullable": False, "metadata": {}},  # Changed from double to long
                    {"name": "hire_date", "type": "string", "nullable": False, "metadata": {}},
                    {"name": "is_active", "type": "boolean", "nullable": False, "metadata": {}},
                    {"name": "created_at", "type": "timestamp", "nullable": False, "metadata": {}},
                    {"name": "email", "type": "string", "nullable": True, "metadata": {}}
                ]
            }),
            "partitionColumns": ["department"],
            "configuration": {
                "delta.autoOptimize.autoCompact": "true",
                "delta.autoOptimize.optimizeWrite": "true"
            },
            "createdTime": txn1_time
        },
        "add": [
            {
                "path": "part-00005-005.parquet",
                "partitionValues": {"department": "finance"},
                "size": 1024 * 1024 * 4,  # 4MB
                "modificationTime": txn5_time,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 1500,
                    "minValues": {"id": 6001, "salary": 60000},
                    "maxValues": {"id": 7500, "salary": 300000},
                    "nullCount": {"id": 0, "salary": 0}
                })
            }
        ],
        "commitInfo": {
            "timestamp": txn5_time,
            "operation": "WRITE",
            "operationParameters": {},
            "isolationLevel": "WriteSerializable",
            "isBlindAppend": False
        }
    }
    
    # Transaction 6: Recent data with small files (compaction opportunity)
    txn6_time = base_time - (5 * 24 * 60 * 60 * 1000)  # 5 days ago
    txn6 = {
        "protocol": {"minReaderVersion": 2, "minWriterVersion": 3},
        "add": [
            {
                "path": "part-00006-006.parquet",
                "partitionValues": {"department": "engineering"},
                "size": 1024 * 512,  # 512KB - small file
                "modificationTime": txn6_time,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 100,
                    "minValues": {"id": 8001, "salary": 70000},
                    "maxValues": {"id": 8100, "salary": 180000},
                    "nullCount": {"id": 0, "salary": 0}
                })
            },
            {
                "path": "part-00007-006.parquet",
                "partitionValues": {"department": "engineering"},
                "size": 1024 * 256,  # 256KB - small file
                "modificationTime": txn6_time,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 50,
                    "minValues": {"id": 8101, "salary": 75000},
                    "maxValues": {"id": 8150, "salary": 190000},
                    "nullCount": {"id": 0, "salary": 0}
                })
            },
            {
                "path": "part-00008-006.parquet",
                "partitionValues": {"department": "marketing"},
                "size": 1024 * 384,  # 384KB - small file
                "modificationTime": txn6_time,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 75,
                    "minValues": {"id": 8201, "salary": 55000},
                    "maxValues": {"id": 8275, "salary": 130000},
                    "nullCount": {"id": 0, "salary": 0}
                })
            }
        ],
        "commitInfo": {
            "timestamp": txn6_time,
            "operation": "WRITE",
            "operationParameters": {},
            "isolationLevel": "WriteSerializable",
            "isBlindAppend": False
        }
    }
    
    # Transaction 7: Most recent - more deletions with deletion vectors
    txn7_time = base_time - (1 * 24 * 60 * 60 * 1000)  # 1 day ago
    txn7 = {
        "protocol": {"minReaderVersion": 2, "minWriterVersion": 3},
        "remove": [
            {
                "path": "part-00001-001.parquet",
                "deletionVector": {
                    "storageType": "u",
                    "pathOrInlineData": "dummy-deletion-vector-data-2",
                    "sizeInBytes": 2048,
                    "cardinality": 100
                },
                "dataChange": True,
                "extendedFileMetadata": True,
                "partitionValues": {"department": "marketing"},
                "size": 1024 * 1024 * 1.5,
                "modificationTime": txn1_time
            }
        ],
        "add": [
            {
                "path": "part-00009-007.parquet",
                "partitionValues": {"department": "marketing"},
                "size": 1024 * 1024 * 1.2,  # 1.2MB
                "modificationTime": txn7_time,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 700,
                    "minValues": {"id": 1001, "salary": 45000},
                    "maxValues": {"id": 1800, "salary": 120000},
                    "nullCount": {"id": 0, "salary": 0}
                })
            }
        ],
        "commitInfo": {
            "timestamp": txn7_time,
            "operation": "DELETE",
            "operationParameters": {"predicate": "id > 1500 AND id < 1600"},
            "isolationLevel": "WriteSerializable",
            "isBlindAppend": False
        }
    }
    
    # Upload transaction logs
    transactions = [txn1, txn2, txn3, txn4, txn5, txn6, txn7]
    
    for i, txn in enumerate(transactions, 1):
        txn_key = f"{table_prefix}/_delta_log/{i:020d}.json"
        txn_content = json.dumps(txn, indent=2)
        
        s3.put_object(
            Bucket=bucket,
            Key=txn_key,
            Body=txn_content.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"  📝 Uploaded transaction log {i}")
    
    # Create some orphaned files (not referenced in transaction logs)
    orphaned_files = [
        "part-orphan-001.parquet",
        "part-orphan-002.parquet", 
        "part-orphan-003.parquet"
    ]
    
    for orphan_file in orphaned_files:
        orphan_key = f"{table_prefix}/{orphan_file}"
        # Create a small dummy parquet file
        dummy_content = b"PAR1" + b"dummy parquet content" * 100
        s3.put_object(
            Bucket=bucket,
            Key=orphan_key,
            Body=dummy_content,
            ContentType='application/octet-stream'
        )
        print(f"  🗑️  Created orphaned file: {orphan_file}")
    
    print(f"\n✅ Advanced Delta Lake table created at: s3://{bucket}/{table_prefix}")
    print("📊 Features included:")
    print("  - Multiple snapshots for time travel analysis")
    print("  - Deletion vectors for performance impact analysis")
    print("  - Schema evolution (non-breaking and breaking changes)")
    print("  - Liquid clustering configuration")
    print("  - Mixed file sizes (small files for compaction analysis)")
    print("  - Partitioned data")
    print("  - Orphaned files")
    print("  - Table constraints in schema")
    
    return f"s3://{bucket}/{table_prefix}"

if __name__ == "__main__":
    table_path = create_advanced_delta_table()
    print(f"\n🎯 Test the advanced table with:")
    print(f"drainage.analyze_table('{table_path}', aws_region='us-east-1')")
