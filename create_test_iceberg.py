#!/usr/bin/env python3
"""
Create a test Iceberg table using PyIceberg for testing the drainage tool
"""
import os
import tempfile
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, LongType, DoubleType
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.catalog.hive import HiveCatalog
import pandas as pd

def create_test_iceberg_table():
    print("🧊 Creating test Iceberg table...")
    
    # Set up S3 configuration
    s3_bucket = "confessions-of-a-data-guy"
    s3_prefix = "picklebob/test-iceberg-table"
    s3_path = f"s3://{s3_bucket}/{s3_prefix}"
    
    print(f"📁 Target S3 path: {s3_path}")
    
    try:
        # Create a temporary directory for the catalog
        with tempfile.TemporaryDirectory() as temp_dir:
            # Configure the catalog
            catalog_config = {
                "type": "hive",
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
                NestedField(6, "created_at", LongType(), required=True),
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
                "department": ["Engineering", "Engineering", "Marketing", "Engineering", "Marketing", "Sales", "Sales", "Engineering", "Marketing", "Sales"],
                "created_at": [1640995200000, 1641081600000, 1641168000000, 1641254400000, 1641340800000, 1641427200000, 1641513600000, 1641600000000, 1641686400000, 1641772800000]
            })
            
            # Insert data
            table.append(test_data)
            print("✅ Test data inserted!")
            
            # Create some additional snapshots by inserting more data
            print("📸 Creating additional snapshots...")
            
            more_data = pd.DataFrame({
                "id": [11, 12, 13],
                "name": ["Kate", "Liam", "Maya"],
                "age": [24, 29, 31],
                "salary": [51000.0, 59000.0, 63000.0],
                "department": ["Engineering", "Marketing", "Sales"],
                "created_at": [1641859200000, 1641945600000, 1642032000000]
            })
            
            table.append(more_data)
            print("✅ Additional data inserted!")
            
            # Get the final S3 path for testing
            final_s3_path = table.location()
            print(f"\n🎯 Test table ready for drainage analysis!")
            print(f"📍 S3 Path: {final_s3_path}")
            print(f"📊 Table has {len(table.scan().to_arrow().to_pandas())} rows")
            print(f"📸 Table has {len(list(table.history()))} snapshots")
            
            return final_s3_path
            
    except Exception as e:
        print(f"❌ Error creating test table: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    s3_path = create_test_iceberg_table()
    if s3_path:
        print(f"\n🧪 Ready to test drainage with: {s3_path}")
    else:
        print("❌ Failed to create test table")
