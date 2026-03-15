# Databricks notebook source
"""
NOTEBOOK: 02_LoadSampleData
Purpose: Load sample CSV data into Bronze layer tables
"""

# COMMAND ----------

print("=" * 60)
print("LOADING SAMPLE DATA INTO BRONZE LAYER")
print("=" * 60)

# COMMAND ----------

import sys
sys.path.append('/Workspace/Shared/src')

from pyspark.sql.functions import current_timestamp

# Configuration
CATALOG = "main"
BRONZE_SCHEMA = "bronze_ecommerce"
DATA_PATH = "/dbfs/data"

print(f"\nConfiguration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {BRONZE_SCHEMA}")
print(f"  Data Path: {DATA_PATH}")

# COMMAND ----------

# Load customer data
print("\n1. Loading customer data...")

customers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{DATA_PATH}/customers.csv")

customers_df = customers_df.withColumn("ingestion_date", current_timestamp())

table_path = f"{CATALOG}.{BRONZE_SCHEMA}.customers"
customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(table_path)

print(f"  ✓ Loaded {customers_df.count()} customer records")
customers_df.display()

# COMMAND ----------

# Load product data
print("\n2. Loading product data...")

products_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{DATA_PATH}/products.csv")

products_df = products_df.withColumn("ingestion_date", current_timestamp())

table_path = f"{CATALOG}.{BRONZE_SCHEMA}.products"
products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(table_path)

print(f"  ✓ Loaded {products_df.count()} product records")
products_df.display()

# COMMAND ----------

# Load order data
print("\n3. Loading order data...")

orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{DATA_PATH}/orders.csv")

orders_df = orders_df.withColumn("ingestion_date", current_timestamp())

table_path = f"{CATALOG}.{BRONZE_SCHEMA}.orders"
orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(table_path)

print(f"  ✓ Loaded {orders_df.count()} order records")
orders_df.display()

# COMMAND ----------

# Summary
print("\n" + "=" * 60)
print("DATA LOADING COMPLETED")
print("=" * 60)
print(f"\nTables created:")
print(f"  - {CATALOG}.{BRONZE_SCHEMA}.customers ({customers_df.count()} records)")
print(f"  - {CATALOG}.{BRONZE_SCHEMA}.products ({products_df.count()} records)")
print(f"  - {CATALOG}.{BRONZE_SCHEMA}.orders ({orders_df.count()} records)")

# MAGIC     StructField("country", StringType()),
# MAGIC     StructField("registration_date", StringType()),
# MAGIC ])
# MAGIC
# MAGIC customers_df = spark.createDataFrame(customers_data, customers_schema)
# MAGIC customers_df.write.mode("overwrite").option("mergeSchema", "true") \
# MAGIC     .saveAsTable(f"{catalog}.{bronze_schema}.bronze_customers")
# MAGIC
# MAGIC print(f"✓ Loaded {customers_df.count()} customer records")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # Load Products data
# MAGIC products_data = [
# MAGIC     ("P001", "Laptop Pro", "Electronics", 1299.99, 50, "S001", "2023-01-01"),
# MAGIC     ("P002", "USB-C Cable", "Accessories", 19.99, 500, "S002", "2023-01-05"),
# MAGIC     ("P003", "Mechanical Keyboard", "Electronics", 149.99, 150, "S001", "2023-01-10"),
# MAGIC     ("P004", "Wireless Mouse", "Accessories", 49.99, 200, "S002", "2023-01-15"),
# MAGIC     ("P005", "Monitor 27\"", "Electronics", 399.99, 75, "S001", "2023-02-01"),
# MAGIC     ("P006", "Desk Lamp", "Office", 79.99, 100, "S003", "2023-02-05"),
# MAGIC     ("P007", "Notebook Set", "Stationery", 24.99, 300, "S003", "2023-02-10"),
# MAGIC     ("P008", "Pen Pack", "Stationery", 9.99, 500, "S003", "2023-02-15"),
# MAGIC     ("P009", "Webcam HD", "Electronics", 89.99, 120, "S002", "2023-03-01"),
# MAGIC     ("P010", "Headphones Pro", "Electronics", 199.99, 180, "S001", "2023-03-05"),
# MAGIC ]
# MAGIC
# MAGIC products_schema = StructType([
# MAGIC     StructField("product_id", StringType()),
# MAGIC     StructField("product_name", StringType()),
# MAGIC     StructField("category", StringType()),
# MAGIC     StructField("price", DoubleType()),
# MAGIC     StructField("stock_quantity", IntegerType()),
# MAGIC     StructField("supplier_id", StringType()),
# MAGIC     StructField("created_date", StringType()),
# MAGIC ])
# MAGIC
# MAGIC products_df = spark.createDataFrame(products_data, products_schema)
# MAGIC products_df.write.mode("overwrite").option("mergeSchema", "true") \
# MAGIC     .saveAsTable(f"{catalog}.{bronze_schema}.bronze_products")
# MAGIC
# MAGIC print(f"✓ Loaded {products_df.count()} product records")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # Load Orders data
# MAGIC orders_data = [
# MAGIC     ("O001", "C001", "P001", 1, "2023-11-01", "2023-11-05", "Delivered", 1299.99),
# MAGIC     ("O002", "C002", "P002", 2, "2023-11-02", "2023-11-03", "Delivered", 39.98),
# MAGIC     ("O003", "C003", "P003", 1, "2023-11-03", "2023-11-08", "In Transit", 149.99),
# MAGIC     ("O004", "C001", "P004", 3, "2023-11-04", "2023-11-07", "Delivered", 149.97),
# MAGIC     ("O005", "C004", "P005", 1, "2023-11-05", "2023-11-12", "Delivered", 399.99),
# MAGIC     ("O006", "C005", "P006", 2, "2023-11-06", "2023-11-10", "Delivered", 159.98),
# MAGIC     ("O007", "C006", "P007", 4, "2023-11-07", "2023-11-09", "Delivered", 99.96),
# MAGIC     ("O008", "C007", "P008", 5, "2023-11-08", "2023-11-11", "Delivered", 49.95),
# MAGIC     ("O009", "C008", "P009", 1, "2023-11-09", "2023-11-15", "Shipped", 89.99),
# MAGIC     ("O010", "C009", "P001", 1, "2023-11-10", None, "Pending", 1299.99),
# MAGIC     ("O011", "C010", "P010", 2, "2023-11-11", "2023-11-16", "Delivered", 399.98),
# MAGIC     ("O012", "C002", "P003", 1, "2023-11-12", "2023-11-18", "Shipped", 149.99),
# MAGIC     ("O013", "C003", "P005", 2, "2023-11-13", None, "Pending", 799.98),
# MAGIC     ("O014", "C004", "P002", 3, "2023-11-14", "2023-11-20", "Delivered", 59.97),
# MAGIC     ("O015", "C005", "P010", 1, "2023-11-15", "2023-11-21", "Delivered", 199.99),
# MAGIC ]
# MAGIC
# MAGIC orders_schema = StructType([
# MAGIC     StructField("order_id", StringType()),
# MAGIC     StructField("customer_id", StringType()),
# MAGIC     StructField("product_id", StringType()),
# MAGIC     StructField("quantity", IntegerType()),
# MAGIC     StructField("order_date", StringType()),
# MAGIC     StructField("delivery_date", StringType()),
# MAGIC     StructField("order_status", StringType()),
# MAGIC     StructField("total_amount", DoubleType()),
# MAGIC ])
# MAGIC
# MAGIC orders_df = spark.createDataFrame(orders_data, orders_schema)
# MAGIC orders_df.write.mode("overwrite").option("mergeSchema", "true") \
# MAGIC     .saveAsTable(f"{catalog}.{bronze_schema}.bronze_orders")
# MAGIC
# MAGIC print(f"✓ Loaded {orders_df.count()} order records")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # Display loaded data summary
# MAGIC print("\n" + "="*50)
# MAGIC print("✓ Sample Data Loaded Successfully!")
# MAGIC print("="*50)
# MAGIC print(f"\nCustomers: {customers_df.count()} records")
# MAGIC print(f"Products: {products_df.count()} records")
# MAGIC print(f"Orders: {orders_df.count()} records")
