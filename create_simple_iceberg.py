#!/usr/bin/env python3
"""
Create a simple test Iceberg table using PyIceberg for testing the drainage tool
"""
import os
import tempfile
import json
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, LongType, DoubleType
from pyiceberg.partitioning import PartitionSpec
import pandas as pd

def create_simple_iceberg_table():
    print("🧊 Creating simple test Iceberg table...")
    
    # Set up S3 configuration
    s3_bucket = "confessions-of-a-data-guy"
    s3_prefix = "picklebob/test-iceberg-simple"
    s3_path = f"s3://{s3_bucket}/{s3_prefix}"
    
    print(f"📁 Target S3 path: {s3_path}")
    
    try:
        # Create a temporary directory for the catalog
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a simple REST catalog configuration
            catalog_config = {
                "type": "rest",
                "uri": f"file://{temp_dir}/catalog",
                "s3.endpoint": "https://s3.us-east-1.amazonaws.com",
                "s3.region": "us-east-1",
                "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID"),
                "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            }
            
            # Load the catalog
            catalog = load_catalog("test_catalog", **catalog_config)
            
            # Define schema
            schema = Schema(
                NestedField(1, "id", IntegerType(), required=True),
                NestedField(2, "name", StringType(), required=True),
                NestedField(3, "age", IntegerType(), required=False),
                NestedField(4, "salary", DoubleType(), required=False),
                NestedField(5, "department", StringType(), required=True),
            )
            
            # Define partitioning
            partition_spec = PartitionSpec(
                NestedField(5, "department", StringType(), required=True)
            )
            
            # Create the table
            table_name = "test_employees"
            print(f"📊 Creating table: {table_name}")
            
            table = catalog.create_table(
                identifier=table_name,
                schema=schema,
                partition_spec=partition_spec,
                location=s3_path
            )
            
            print("✅ Table created successfully!")
            print(f"📍 Table location: {table.location()}")
            
            # Insert some test data
            print("📝 Inserting test data...")
            
            # Create test data
            test_data = pd.DataFrame({
                "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"],
                "age": [25, 30, 35, 28, 32, 27, 29, 31, 26, 33],
                "salary": [50000.0, 60000.0, 70000.0, 55000.0, 65000.0, 52000.0, 58000.0, 62000.0, 48000.0, 68000.0],
                "department": ["Engineering", "Engineering", "Marketing", "Engineering", "Marketing", "Sales", "Sales", "Engineering", "Marketing", "Sales"]
            })
            
            # Insert data
            table.append(test_data)
            print("✅ Test data inserted!")
            
            # Get the final S3 path for testing
            final_s3_path = table.location()
            print(f"\n🎯 Test table ready for drainage analysis!")
            print(f"📍 S3 Path: {final_s3_path}")
            print(f"📊 Table has {len(table.scan().to_arrow().to_pandas())} rows")
            
            return final_s3_path
            
    except Exception as e:
        print(f"❌ Error creating test table: {e}")
        import traceback
        traceback.print_exc()
        
        # Fallback: create a simple S3 structure that looks like Iceberg
        print("\n🔄 Trying fallback approach - creating Iceberg-like structure...")
        return create_fallback_iceberg_structure(s3_path)

def create_fallback_iceberg_structure(s3_path):
    """Create a simple S3 structure that mimics Iceberg for testing"""
    try:
        import boto3
        
        # Parse S3 path
        s3_path_clean = s3_path.replace("s3://", "")
        bucket, prefix = s3_path_clean.split("/", 1)
        
        # Create S3 client
        s3_client = boto3.client('s3')
        
        # Create metadata.json file
        metadata = {
            "format-version": 2,
            "table-uuid": "test-uuid-12345",
            "location": s3_path,
            "last-updated-ms": 1640995200000,
            "last-column-id": 5,
            "schema": {
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "type": "int", "required": True},
                    {"id": 2, "name": "name", "type": "string", "required": True},
                    {"id": 3, "name": "age", "type": "int", "required": False},
                    {"id": 4, "name": "salary", "type": "double", "required": False},
                    {"id": 5, "name": "department", "type": "string", "required": True}
                ]
            },
            "current-schema-id": 0,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "type": "int", "required": True},
                        {"id": 2, "name": "name", "type": "string", "required": True},
                        {"id": 3, "name": "age", "type": "int", "required": False},
                        {"id": 4, "name": "salary", "type": "double", "required": False},
                        {"id": 5, "name": "department", "type": "string", "required": True}
                    ]
                }
            ],
            "partition-spec": [
                {"field-id": 5, "name": "department", "transform": "identity"}
            ],
            "default-spec-id": 0,
            "partition-specs": [
                {
                    "spec-id": 0,
                    "fields": [
                        {"field-id": 5, "name": "department", "transform": "identity"}
                    ]
                }
            ],
            "last-partition-id": 1000,
            "default-sort-order-id": 0,
            "sort-orders": [
                {
                    "order-id": 0,
                    "fields": []
                }
            ],
            "properties": {},
            "current-snapshot-id": 1,
            "refs": {
                "main": {
                    "snapshot-id": 1,
                    "type": "branch"
                }
            },
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "timestamp-ms": 1640995200000,
                    "summary": {
                        "operation": "append",
                        "added-data-files": 1,
                        "added-records": 10,
                        "added-files-size": 1024
                    },
                    "manifest-list": f"{prefix}/metadata/snap-1-manifest-list.avro",
                    "schema-id": 0
                }
            ],
            "snapshot-log": [
                {
                    "timestamp-ms": 1640995200000,
                    "snapshot-id": 1
                }
            ],
            "metadata-log": [
                {
                    "timestamp-ms": 1640995200000,
                    "metadata-file": f"{prefix}/metadata/metadata.json"
                }
            ]
        }
        
        # Upload metadata.json
        metadata_key = f"{prefix}/metadata/metadata.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Created metadata.json at s3://{bucket}/{metadata_key}")
        
        # Create a simple manifest list
        manifest_list = {
            "manifest-list": f"{prefix}/metadata/snap-1-manifest-list.avro",
            "manifests": [
                {
                    "manifest_path": f"{prefix}/metadata/manifest-1.avro",
                    "manifest_length": 1024,
                    "partition_spec_id": 0,
                    "added_snapshot_id": 1,
                    "added_data_files_count": 1,
                    "added_rows_count": 10,
                    "existing_data_files_count": 0,
                    "existing_rows_count": 0,
                    "deleted_data_files_count": 0,
                    "deleted_rows_count": 0,
                    "partitions": [
                        {
                            "contains_null": False,
                            "contains_nan": False,
                            "lower_bound": "Engineering",
                            "upper_bound": "Sales"
                        }
                    ],
                    "added_files": [
                        {
                            "content": 0,
                            "file_path": f"{prefix}/data/part-00000-abc123.parquet",
                            "file_format": "PARQUET",
                            "partition": {"department": "Engineering"},
                            "record_count": 5,
                            "file_size_in_bytes": 512,
                            "column_sizes": {"id": 20, "name": 50, "age": 20, "salary": 40, "department": 30},
                    "value_counts": {"id": 5, "name": 5, "age": 5, "salary": 5, "department": 5},
                    "null_value_counts": {"id": 0, "name": 0, "age": 0, "salary": 0, "department": 0},
                    "nan_value_counts": {"id": 0, "name": 0, "age": 0, "salary": 0, "department": 0},
                    "distinct_counts": {"id": 5, "name": 5, "age": 5, "salary": 5, "department": 2},
                    "lower_bounds": {"id": "1", "name": "Alice", "age": "25", "salary": "48000.0", "department": "Engineering"},
                    "upper_bounds": {"id": "5", "name": "Eve", "age": "32", "salary": "70000.0", "department": "Marketing"}
                        }
                    ]
                }
            ]
        }
        
        # Upload manifest list
        manifest_list_key = f"{prefix}/metadata/snap-1-manifest-list.avro"
        s3_client.put_object(
            Bucket=bucket,
            Key=manifest_list_key,
            Body=json.dumps(manifest_list, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Created manifest list at s3://{bucket}/{manifest_list_key}")
        
        # Create a dummy parquet file
        parquet_key = f"{prefix}/data/part-00000-abc123.parquet"
        s3_client.put_object(
            Bucket=bucket,
            Key=parquet_key,
            Body=b"dummy parquet content for testing",
            ContentType='application/octet-stream'
        )
        print(f"✅ Created dummy parquet file at s3://{bucket}/{parquet_key}")
        
        print(f"\n🎯 Fallback Iceberg structure created!")
        print(f"📍 S3 Path: {s3_path}")
        
        return s3_path
        
    except Exception as e:
        print(f"❌ Fallback also failed: {e}")
        return None

if __name__ == "__main__":
    s3_path = create_simple_iceberg_table()
    if s3_path:
        print(f"\n🧪 Ready to test drainage with: {s3_path}")
    else:
        print("❌ Failed to create test table")
