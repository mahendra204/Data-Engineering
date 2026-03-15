# Databricks notebook source
"""
NOTEBOOK: 01_Setup
Purpose: Initialize pipeline environment and create schemas/tables
"""

# COMMAND ----------

print("=" * 60)
print("DATABRICKS DECLARATIVE PIPELINE SETUP")
print("=" * 60)

# COMMAND ----------

# Configuration
CATALOG = "main"
BRONZE_SCHEMA = "bronze_ecommerce"
SILVER_SCHEMA = "silver_ecommerce"
GOLD_SCHEMA = "gold_ecommerce"

print(f"\nConfiguration:")
print(f"  Catalog: {CATALOG}")
print(f"  Bronze Schema: {BRONZE_SCHEMA}")
print(f"  Silver Schema: {SILVER_SCHEMA}")
print(f"  Gold Schema: {GOLD_SCHEMA}")

# COMMAND ----------

# Create schemas
print("\nCreating schemas...")

sql_statements = [
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}",
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}",
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}"
]

for sql in sql_statements:
    try:
        spark.sql(sql)
        print(f"  ✓ {sql}")
    except Exception as e:
        print(f"  ✗ Error: {str(e)}")

# COMMAND ----------

# Verify schemas
print("\nVerifying schemas...")
schemas = spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
for schema in schemas:
    schema_name = schema["namespace"]
    if any(s in schema_name for s in [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]):
        print(f"  ✓ Schema exists: {schema_name}")

# COMMAND ----------

# Display Spark configuration
print("\nSpark Configuration:")
print(f"  Spark Version: {spark.version}")

# COMMAND ----------

# Set up Delta Live Tables pipeline configuration
print("\nDelta Live Tables Configuration:")
print(f"  Target Catalog: {CATALOG}")
print(f"  DLT Pipeline Notebook: dlt_pipeline.py")

# COMMAND ----------

print("\n" + "=" * 60)
print("SETUP COMPLETED SUCCESSFULLY")
print("=" * 60)

# MAGIC bronze_path = f"{base_path}/bronze"
# MAGIC silver_path = f"{base_path}/silver"
# MAGIC gold_path = f"{base_path}/gold"
# MAGIC checkpoint_path = f"{base_path}/checkpoints"
# MAGIC
# MAGIC for path in [bronze_path, silver_path, gold_path, checkpoint_path]:
# MAGIC     dbutils.fs.mkdirs(path)
# MAGIC     print(f"✓ Directory created: {path}")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # Sample data configuration
# MAGIC sample_data = {
# MAGIC     "customers": {
# MAGIC         "count": 10,
# MAGIC         "schema": "STRUCT<customer_id:STRING, first_name:STRING, last_name:STRING, email:STRING, phone:STRING, city:STRING, country:STRING, registration_date:STRING>"
# MAGIC     },
# MAGIC     "products": {
# MAGIC         "count": 10,
# MAGIC         "schema": "STRUCT<product_id:STRING, product_name:STRING, category:STRING, price:DOUBLE, stock_quantity:INT, supplier_id:STRING, created_date:STRING>"
# MAGIC     },
# MAGIC     "orders": {
# MAGIC         "count": 15,
# MAGIC         "schema": "STRUCT<order_id:STRING, customer_id:STRING, product_id:STRING, quantity:INT, order_date:STRING, delivery_date:STRING, order_status:STRING, total_amount:DOUBLE>"
# MAGIC     }
# MAGIC }
# MAGIC
# MAGIC print("Data Pipeline Configuration:")
# MAGIC for table, config in sample_data.items():
# MAGIC     print(f"  - {table}: {config['count']} records")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # Create metadata table for tracking
# MAGIC metadata_sql = f"""
# MAGIC CREATE TABLE IF NOT EXISTS {catalog_name}.{bronze_schema}.pipeline_metadata (
# MAGIC     pipeline_run_id STRING,
# MAGIC     table_name STRING,
# MAGIC     record_count LONG,
# MAGIC     run_timestamp TIMESTAMP,
# MAGIC     status STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC """
# MAGIC
# MAGIC spark.sql(metadata_sql)
# MAGIC print("✓ Metadata table created")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # Log initialization successful
# MAGIC print("\n" + "="*50)
# MAGIC print("✓ Pipeline Setup Completed Successfully!")
# MAGIC print("="*50)
# MAGIC print("\nCreated:")
# MAGIC print(f"  - Schemas: {bronze_schema}, {silver_schema}, {gold_schema}")
# MAGIC print(f"  - Directories: {bronze_path}, {silver_path}, {gold_path}")
# MAGIC print(f"  - Metadata table: pipeline_metadata")
