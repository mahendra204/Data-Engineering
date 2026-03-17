"""
Declarative Data Pipeline using Delta Live Tables (DLT)
Complete end-to-end DLT implementation with medallion architecture
Bronze → Silver → Gold layers with data quality expectations
"""

import dlt
from pyspark.sql.functions import (
    col, to_date, when, sum as spark_sum, count, avg, 
    date_format, year, month, row_number, 
    dense_rank, lag, lead, current_timestamp
)
from pyspark.sql.window import Window
from datetime import datetime

# ============================================================================
# BRONZE LAYER - Raw Data Ingestion (No Transformations)
# ============================================================================

@dlt.table(
    comment="Raw customers data from source system - no transformations",
    table_properties={
        "quality": "bronze",
        "owner": "data_engineering",
        "purpose": "Raw customer source data"
    }
)
def bronze_customers():
    """Ingests raw customer data directly from source."""
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/dbfs/data/customers.csv")


@dlt.table(
    comment="Raw products data from source system - no transformations",
    table_properties={
        "quality": "bronze",
        "owner": "data_engineering",
        "purpose": "Raw product source data"
    }
)
def bronze_products():
    """Ingests raw product data directly from source."""
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/dbfs/data/products.csv")


@dlt.table(
    comment="Raw orders data from source system - no transformations",
    table_properties={
        "quality": "bronze",
        "owner": "data_engineering",
        "purpose": "Raw order transaction data"
    }
)
def bronze_orders():
    """Ingests raw order data directly from source."""
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/dbfs/data/orders.csv")


# ============================================================================
# SILVER LAYER - Data Cleaning & Validation with Quality Expectations
# ============================================================================

@dlt.table(
    comment="Cleaned and validated customer data with quality checks",
    table_properties={
        "quality": "silver",
        "owner": "data_engineering",
        "sla": "99.9% accuracy"
    }
)
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email", "email IS NOT NULL AND email LIKE '%@%.%'")
@dlt.expect("valid_names", "first_name IS NOT NULL AND last_name IS NOT NULL")
@dlt.expect("valid_phone", "phone IS NOT NULL")
def silver_customers():
    """Cleans and validates customer data with quality expectations."""
    return dlt.read("bronze_customers") \
        .select(
            col("customer_id").cast("int").alias("customer_id"),
            col("first_name").cast("string").alias("first_name"),
            col("last_name").cast("string").alias("last_name"),
            col("email").cast("string").alias("email"),
            col("phone").cast("string").alias("phone"),
            col("city").cast("string").alias("city"),
            col("country").cast("string").alias("country"),
            col("registration_date").cast("string").alias("registration_date")
        ) \
        .filter(
            col("customer_id").isNotNull() & 
            col("email").isNotNull() & 
            col("first_name").isNotNull()
        ) \
        .dropDuplicates(["customer_id"])


@dlt.table(
    comment="Cleaned and validated product data with quality checks",
    table_properties={
        "quality": "silver",
        "owner": "data_engineering",
        "sla": "99.9% accuracy"
    }
)
@dlt.expect("valid_product_id", "product_id IS NOT NULL")
@dlt.expect("valid_price", "price > 0")
@dlt.expect("valid_stock", "stock_quantity >= 0")
@dlt.expect("valid_category", "category IS NOT NULL")
def silver_products():
    """Cleans and validates product data with quality expectations."""
    return dlt.read("bronze_products") \
        .select(
            col("product_id").cast("int").alias("product_id"),
            col("product_name").cast("string").alias("product_name"),
            col("category").cast("string").alias("category"),
            col("price").cast("decimal(10,2)").alias("price"),
            col("stock_quantity").cast("int").alias("stock_quantity"),
            col("supplier_id").cast("int").alias("supplier_id"),
            col("created_date").cast("string").alias("created_date")
        ) \
        .filter(
            (col("product_id").isNotNull()) & 
            (col("price") > 0) & 
            (col("stock_quantity") >= 0) &
            (col("category").isNotNull())
        ) \
        .dropDuplicates(["product_id"])


@dlt.table(
    comment="Cleaned and validated order data with quality checks",
    table_properties={
        "quality": "silver",
        "owner": "data_engineering",
        "sla": "99.9% accuracy"
    }
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_amount", "order_amount > 0")
@dlt.expect("valid_date", "order_date IS NOT NULL")
def silver_orders():
    """Cleans and validates order data with quality expectations."""
    return dlt.read("bronze_orders") \
        .select(
            col("order_id").cast("long").alias("order_id"),
            col("customer_id").cast("int").alias("customer_id"),
            col("product_id").cast("int").alias("product_id"),
            col("order_date").cast("string").alias("order_date"),
            col("order_amount").cast("decimal(12,2)").alias("order_amount"),
            col("quantity").cast("int").alias("quantity"),
            col("status").cast("string").alias("status")
        ) \
        .filter(
            (col("order_id").isNotNull()) & 
            (col("customer_id").isNotNull()) &
            (col("order_amount") > 0) &
            (col("order_date").isNotNull())
        ) \
        .dropDuplicates(["order_id"])


# ============================================================================
# GOLD LAYER - Business Analytics & Aggregations
# ============================================================================

@dlt.table(
    comment="Customer order history with summaries",
    table_properties={
        "quality": "gold",
        "owner": "analytics",
        "refresh": "daily"
    }
)
def gold_customer_orders():
    """Aggregates customer orders with key metrics."""
    return dlt.read("silver_customers") \
        .join(
            dlt.read("silver_orders"),
            on="customer_id",
            how="left"
        ) \
        .groupBy("customer_id") \
        .agg(
            col("first_name").first().alias("first_name"),
            col("last_name").first().alias("last_name"),
            col("email").first().alias("email"),
            count("order_id").alias("total_orders"),
            spark_sum("order_amount").alias("total_spent"),
            avg("order_amount").alias("avg_order_value"),
            col("registration_date").first().alias("registration_date")
        ) \
        .filter(col("total_orders") > 0)


@dlt.table(
    comment="Product performance metrics",
    table_properties={
        "quality": "gold",
        "owner": "analytics",
        "refresh": "daily"
    }
)
def gold_product_performance():
    """Analyzes product performance including sales metrics."""
    return dlt.read("silver_products") \
        .join(
            dlt.read("silver_orders"),
            on="product_id",
            how="left"
        ) \
        .groupBy("product_id") \
        .agg(
            col("product_name").first().alias("product_name"),
            col("category").first().alias("category"),
            col("price").first().alias("price"),
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_sum("order_amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("order_amount").alias("avg_order_value")
        ) \
        .filter(col("total_quantity_sold") > 0)


@dlt.table(
    comment="Monthly revenue aggregation by customer",
    table_properties={
        "quality": "gold",
        "owner": "analytics",
        "refresh": "daily"
    }
)
def gold_monthly_customer_revenue():
    """Aggregates monthly revenue by customer."""
    return dlt.read("silver_customers") \
        .join(
            dlt.read("silver_orders"),
            on="customer_id",
            how="inner"
        ) \
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            date_format(col("order_date"), "yyyy-MM").alias("order_month"),
            col("order_amount")
        ) \
        .groupBy("customer_id", "first_name", "last_name", "email", "order_month") \
        .agg(
            spark_sum("order_amount").alias("monthly_revenue"),
            count("order_id").alias("order_count")
        ) \
        .orderBy("customer_id", "order_month")


@dlt.table(
    comment="Category-wise sales summary",
    table_properties={
        "quality": "gold",
        "owner": "analytics",
        "refresh": "daily"
    }
)
def gold_category_summary():
    """Summarizes sales by product category."""
    return dlt.read("silver_products") \
        .join(
            dlt.read("silver_orders"),
            on="product_id",
            how="inner"
        ) \
        .groupBy("category") \
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("order_amount").alias("total_revenue"),
            spark_sum("quantity").alias("total_quantity"),
            avg("order_amount").alias("avg_order_value"),
            count("product_id").alias("unique_products")
        ) \
        .orderBy(col("total_revenue").desc())


@dlt.table(
    comment="Top customers by spending",
    table_properties={
        "quality": "gold",
        "owner": "analytics",
        "refresh": "daily"
    }
)
def gold_top_customers():
    """Identifies top spending customers with ranking."""
    customer_spending = dlt.read("silver_customers") \
        .join(
            dlt.read("silver_orders"),
            on="customer_id",
            how="left"
        ) \
        .groupBy("customer_id", "email", "first_name", "last_name") \
        .agg(
            spark_sum("order_amount").alias("total_spent"),
            count("order_id").alias("order_count")
        )
    
    window_spec = Window.orderBy(col("total_spent").desc())
    
    return customer_spending \
        .withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 100)


@dlt.table(
    comment="Complete order enrichment with customer and product details",
    table_properties={
        "quality": "gold",
        "owner": "analytics",
        "refresh": "hourly"
    }
)
def gold_enriched_orders():
    """Enriches orders with customer and product information."""
    return dlt.read("silver_orders") \
        .join(
            dlt.read("silver_customers").select(
                col("customer_id"),
                col("first_name"),
                col("last_name"),
                col("email"),
                col("city"),
                col("country")
            ),
            on="customer_id",
            how="left"
        ) \
        .join(
            dlt.read("silver_products").select(
                col("product_id"),
                col("product_name"),
                col("category"),
                col("price")
            ),
            on="product_id",
            how="left"
        ) \
        .select(
            col("order_id"),
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("city"),
            col("country"),
            col("product_id"),
            col("product_name"),
            col("category"),
            col("order_date"),
            col("order_amount"),
            col("quantity"),
            col("status"),
            (col("order_amount") / col("quantity")).alias("unit_price"),
            ((col("order_amount") / col("quantity")) - col("price")).alias("margin_per_unit")
        )


if __name__ == "__main__":
    print("DLT Pipeline definitions loaded successfully")
