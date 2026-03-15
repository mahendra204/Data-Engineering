# Databricks notebook source
"""
NOTEBOOK: 03_Analytics
Purpose: Run analytics queries on processed Gold layer data
"""

# COMMAND ----------

print("=" * 60)
print("GOLD LAYER ANALYTICS & QUERIES")
print("=" * 60)

# COMMAND ----------

from pyspark.sql.functions import col

# Configuration
CATALOG = "main"
GOLD_SCHEMA = "gold_ecommerce"

print(f"\nConfiguration:")
print(f"  Catalog: {CATALOG}")
print(f"  Gold Schema: {GOLD_SCHEMA}")

# COMMAND ----------

# 1. Top Customers by Spending
print("\n1. TOP 10 CUSTOMERS BY SPENDING")
print("-" * 60)

top_customers_query = f"""
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    total_orders,
    total_spent,
    avg_order_value,
    rank
FROM {CATALOG}.{GOLD_SCHEMA}.gold_top_customers
ORDER BY rank ASC
LIMIT 10
"""

top_customers = spark.sql(top_customers_query)
top_customers.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Product Sales Performance
# MAGIC SELECT 
# MAGIC     p.product_id,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     p.price,
# MAGIC     COUNT(o.order_id) AS units_sold,
# MAGIC     SUM(o.quantity) AS total_quantity,
# MAGIC     SUM(o.total_amount) AS total_revenue,
# MAGIC     ROUND(SUM(o.total_amount) / COUNT(o.order_id), 2) AS avg_order_value
# MAGIC FROM main.bronze_ecommerce.bronze_products p
# MAGIC LEFT JOIN main.bronze_ecommerce.bronze_orders o
# MAGIC     ON p.product_id = o.product_id
# MAGIC GROUP BY p.product_id, p.product_name, p.category, p.price
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Order Status Distribution
# MAGIC SELECT 
# MAGIC     order_status,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     SUM(total_amount) AS revenue,
# MAGIC     SUM(quantity) AS quantity,
# MAGIC     ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM main.bronze_ecommerce.bronze_orders), 2) AS percentage
# MAGIC FROM main.bronze_ecommerce.bronze_orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY order_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily Sales Trend
# MAGIC SELECT 
# MAGIC     order_date,
# MAGIC     COUNT(*) AS orders,
# MAGIC     SUM(total_amount) AS revenue,
# MAGIC     SUM(quantity) AS units,
# MAGIC     ROUND(AVG(total_amount), 2) AS avg_order_value
# MAGIC FROM main.bronze_ecommerce.bronze_orders
# MAGIC GROUP BY order_date
# MAGIC ORDER BY order_date DESC

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # Category-wise Revenue Distribution
# MAGIC category_revenue = spark.sql("""
# MAGIC     SELECT 
# MAGIC         p.category,
# MAGIC         COUNT(o.order_id) AS total_orders,
# MAGIC         SUM(o.total_amount) AS total_revenue,
# MAGIC         SUM(o.quantity) AS total_quantity
# MAGIC     FROM main.bronze_ecommerce.bronze_products p
# MAGIC     LEFT JOIN main.bronze_ecommerce.bronze_orders o ON p.product_id = o.product_id
# MAGIC     GROUP BY p.category
# MAGIC     ORDER BY total_revenue DESC
# MAGIC """)
# MAGIC
# MAGIC display(category_revenue)
