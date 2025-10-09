#!/usr/bin/env python3
"""
Create a Delta Lake table with liquid clustering for testing the drainage tool
"""
import os
import tempfile
import json
import boto3
from datetime import datetime

def create_clustered_delta_table():
    print("🧊 Creating Delta Lake table with liquid clustering...")
    
    # Set up S3 configuration
    s3_bucket = "confessions-of-a-data-guy"
    s3_prefix = "picklebob/test-delta-clustered"
    s3_path = f"s3://{s3_bucket}/{s3_prefix}"
    
    print(f"📁 Target S3 path: {s3_path}")
    
    try:
        # Create S3 client
        s3_client = boto3.client('s3')
        
        # Create Delta Lake table structure with liquid clustering
        print("📊 Creating Delta Lake table structure...")
        
        # Create _delta_log directory
        delta_log_prefix = f"{s3_prefix}/_delta_log"
        
        # Create initial transaction log (00000000000000000000.json)
        initial_txn = {
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7
            },
            "metaData": {
                "id": "test-delta-clustered-uuid",
                "name": "test_delta_clustered",
                "description": "Test Delta Lake table with liquid clustering",
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": json.dumps({
                    "type": "struct",
                    "fields": [
                        {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                        {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                        {"name": "age", "type": "integer", "nullable": True, "metadata": {}},
                        {"name": "salary", "type": "double", "nullable": True, "metadata": {}},
                        {"name": "department", "type": "string", "nullable": True, "metadata": {}},
                        {"name": "created_at", "type": "long", "nullable": True, "metadata": {}}
                    ]
                }),
                "partitionColumns": [],
                "configuration": {
                    "delta.clustering.columns": "department,age"
                },
                "clusterBy": ["department", "age"],
                "createdTime": int(datetime.now().timestamp() * 1000)
            }
        }
        
        # Upload initial transaction log
        initial_txn_key = f"{delta_log_prefix}/00000000000000000000.json"
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=initial_txn_key,
            Body=json.dumps(initial_txn, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Created initial transaction log: s3://{s3_bucket}/{initial_txn_key}")
        
        # Create a second transaction log with data additions
        add_actions = []
        for i in range(1, 11):  # Create 10 data files
            add_actions.append({
                "add": {
                    "path": f"data/part-{i:05d}-{i:08x}-{i:08x}-c000.snappy.parquet",
                    "partitionValues": {},
                    "size": 1024 * i,  # Varying file sizes
                    "modificationTime": int(datetime.now().timestamp() * 1000),
                    "dataChange": True,
                    "stats": json.dumps({
                        "numRecords": 100 * i,
                        "minValues": {
                            "id": i,
                            "name": f"user{i}",
                            "age": 20 + i,
                            "salary": 30000.0 + i * 1000,
                            "department": "Engineering" if i % 2 == 0 else "Marketing",
                            "created_at": int(datetime.now().timestamp() * 1000)
                        },
                        "maxValues": {
                            "id": i + 99,
                            "name": f"user{i + 99}",
                            "age": 20 + i + 99,
                            "salary": 30000.0 + (i + 99) * 1000,
                            "department": "Engineering" if i % 2 == 0 else "Marketing",
                            "created_at": int(datetime.now().timestamp() * 1000)
                        },
                        "nullCount": {
                            "id": 0,
                            "name": 0,
                            "age": 0,
                            "salary": 0,
                            "department": 0,
                            "created_at": 0
                        }
                    }),
                    "partitionValues": {},
                    "tags": {
                        "CLUSTERING_COLUMNS": "department,age"
                    }
                }
            })
        
        # Create second transaction log
        second_txn = {
            "add": add_actions,
            "commitInfo": {
                "timestamp": int(datetime.now().timestamp() * 1000),
                "operation": "WRITE",
                "operationParameters": {
                    "mode": "Append",
                    "partitionBy": "[]"
                },
                "isolationLevel": "WriteSerializable",
                "isBlindAppend": True,
                "operationMetrics": {
                    "numFiles": "10",
                    "numOutputRows": "5500",
                    "numOutputBytes": "56320"
                },
                "engineInfo": "Apache-Spark/3.5.0 Delta-Lake/3.1.0",
                "txnId": "00000000000000000001"
            }
        }
        
        second_txn_key = f"{delta_log_prefix}/00000000000000000001.json"
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=second_txn_key,
            Body=json.dumps(second_txn, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Created second transaction log: s3://{s3_bucket}/{second_txn_key}")
        
        # Create some dummy parquet files
        print("📁 Creating dummy data files...")
        for i in range(1, 11):
            parquet_key = f"{s3_prefix}/data/part-{i:05d}-{i:08x}-{i:08x}-c000.snappy.parquet"
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=parquet_key,
                Body=f"dummy parquet content for file {i}".encode(),
                ContentType='application/octet-stream'
            )
        
        print(f"✅ Created 10 dummy parquet files")
        
        # Create a checkpoint file
        checkpoint = {
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7
            },
            "metaData": initial_txn["metaData"],
            "add": add_actions,
            "remove": [],
            "txn": {
                "appId": "test-app",
                "version": 1,
                "lastUpdated": int(datetime.now().timestamp() * 1000)
            }
        }
        
        checkpoint_key = f"{delta_log_prefix}/00000000000000000001.checkpoint.parquet"
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=checkpoint_key,
            Body=json.dumps(checkpoint, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Created checkpoint: s3://{s3_bucket}/{checkpoint_key}")
        
        print(f"\n🎯 Clustered Delta Lake table created successfully!")
        print(f"📍 S3 Path: {s3_path}")
        print(f"🔧 Clustering columns: department, age")
        print(f"📊 Data files: 10")
        print(f"📋 Transaction logs: 2")
        
        return s3_path
        
    except Exception as e:
        print(f"❌ Error creating clustered Delta table: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    s3_path = create_clustered_delta_table()
    if s3_path:
        print(f"\n🧪 Ready to test drainage with: {s3_path}")
    else:
        print("❌ Failed to create clustered Delta table")
