"""
Spark Lakeflow Data Pipeline Implementation
Production-ready Spark pipeline without DLT using Delta Lake Lakeflow patterns
Supports medallion architecture with streaming and batch processing
"""

from typing import Optional, Dict, List
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, to_timestamp, when, sum as spark_sum, count, avg,
    date_format, year, month, row_number, dense_rank,
    current_timestamp, coalesce, lit, cast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DoubleType, LongType
)
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkLakeflowPipeline:
    """
    Production-grade Spark Lakeflow pipeline implementation.
    Handles data ingestion, transformation, and aggregation.
    """
    
    def __init__(
        self,
        catalog: str = "main",
        bronze_schema: str = "bronze_ecommerce",
        silver_schema: str = "silver_ecommerce",
        gold_schema: str = "gold_ecommerce",
        data_path: str = "/dbfs/data"
    ):
        """
        Initialize Spark Lakeflow Pipeline.
        
        Args:
            catalog: Delta Lake catalog name
            bronze_schema: Bronze layer schema
            silver_schema: Silver layer schema
            gold_schema: Gold layer schema
            data_path: Base path for all data
        """
        self.spark = SparkSession.builder \
            .appName("SparkLakeflowPipeline") \
            .config("spark.databricks.delta.schema.autoMigrate.enabled", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .getOrCreate()
        
        self.catalog = catalog
        self.bronze_schema = bronze_schema
        self.silver_schema = silver_schema
        self.gold_schema = gold_schema
        self.data_path = data_path
        
        logger.info(f"Spark Lakeflow Pipeline initialized with {catalog}")
    
    # =========================================================================
    # SCHEMA DEFINITIONS
    # =========================================================================
    
    @staticmethod
    def get_customer_schema() -> StructType:
        """Define schema for customer data."""
        return StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("email", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("registration_date", StringType(), True)
        ])
    
    @staticmethod
    def get_product_schema() -> StructType:
        """Define schema for product data."""
        return StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("price", DecimalType(10, 2), False),
            StructField("stock_quantity", IntegerType(), False),
            StructField("supplier_id", IntegerType(), True),
            StructField("created_date", StringType(), True)
        ])
    
    @staticmethod
    def get_order_schema() -> StructType:
        """Define schema for order data."""
        return StructType([
            StructField("order_id", LongType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("order_date", StringType(), False),
            StructField("order_amount", DecimalType(12, 2), False),
            StructField("quantity", IntegerType(), False),
            StructField("status", StringType(), True)
        ])
    
    # =========================================================================
    # BRONZE LAYER - Data Ingestion
    # =========================================================================
    
    def ingest_customers_bronze(self, file_path: Optional[str] = None) -> DataFrame:
        """Ingest customer data to bronze layer."""
        if file_path is None:
            file_path = f"{self.data_path}/customers.csv"
        
        logger.info(f"Ingesting customer data from {file_path}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .csv(file_path)
        
        df = df.withColumn("ingestion_date", current_timestamp())
        
        table_path = f"{self.catalog}.{self.bronze_schema}.customers"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_path)
        
        logger.info(f"Customer data ingested: {df.count()} records")
        return df
    
    def ingest_products_bronze(self, file_path: Optional[str] = None) -> DataFrame:
        """Ingest product data to bronze layer."""
        if file_path is None:
            file_path = f"{self.data_path}/products.csv"
        
        logger.info(f"Ingesting product data from {file_path}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .csv(file_path)
        
        df = df.withColumn("ingestion_date", current_timestamp())
        
        table_path = f"{self.catalog}.{self.bronze_schema}.products"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_path)
        
        logger.info(f"Product data ingested: {df.count()} records")
        return df
    
    def ingest_orders_bronze(self, file_path: Optional[str] = None) -> DataFrame:
        """Ingest order data to bronze layer."""
        if file_path is None:
            file_path = f"{self.data_path}/orders.csv"
        
        logger.info(f"Ingesting order data from {file_path}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .csv(file_path)
        
        df = df.withColumn("ingestion_date", current_timestamp())
        
        table_path = f"{self.catalog}.{self.bronze_schema}.orders"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_path)
        
        logger.info(f"Order data ingested: {df.count()} records")
        return df
    
    # =========================================================================
    # SILVER LAYER - Data Cleaning & Transformation
    # =========================================================================
    
    def transform_customers_silver(self) -> DataFrame:
        """Clean and transform customer data."""
        logger.info("Transforming customer data to silver layer")
        
        df = self.spark.table(f"{self.catalog}.{self.bronze_schema}.customers")
        
        df = df.select(
            col("customer_id").cast("int"),
            col("first_name").cast("string"),
            col("last_name").cast("string"),
            col("email").cast("string"),
            col("phone").cast("string"),
            col("city").cast("string"),
            col("country").cast("string"),
            col("registration_date").cast("string")
        ) \
        .filter(
            col("customer_id").isNotNull() &
            col("email").isNotNull() &
            col("first_name").isNotNull()
        ) \
        .dropDuplicates(["customer_id"]) \
        .withColumn("processed_date", current_timestamp())
        
        table_path = f"{self.catalog}.{self.silver_schema}.customers"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .partitionBy("country") \
            .saveAsTable(table_path)
        
        logger.info(f"Customer silver data created: {df.count()} records")
        return df
    
    def transform_products_silver(self) -> DataFrame:
        """Clean and transform product data."""
        logger.info("Transforming product data to silver layer")
        
        df = self.spark.table(f"{self.catalog}.{self.bronze_schema}.products")
        
        df = df.select(
            col("product_id").cast("int"),
            col("product_name").cast("string"),
            col("category").cast("string"),
            col("price").cast("decimal(10,2)"),
            col("stock_quantity").cast("int"),
            col("supplier_id").cast("int"),
            col("created_date").cast("string")
        ) \
        .filter(
            (col("product_id").isNotNull()) &
            (col("price") > 0) &
            (col("category").isNotNull())
        ) \
        .dropDuplicates(["product_id"]) \
        .withColumn("processed_date", current_timestamp())
        
        table_path = f"{self.catalog}.{self.silver_schema}.products"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .partitionBy("category") \
            .saveAsTable(table_path)
        
        logger.info(f"Product silver data created: {df.count()} records")
        return df
    
    def transform_orders_silver(self) -> DataFrame:
        """Clean and transform order data."""
        logger.info("Transforming order data to silver layer")
        
        df = self.spark.table(f"{self.catalog}.{self.bronze_schema}.orders")
        
        df = df.select(
            col("order_id").cast("long"),
            col("customer_id").cast("int"),
            col("product_id").cast("int"),
            col("order_date").cast("string"),
            col("order_amount").cast("decimal(12,2)"),
            col("quantity").cast("int"),
            col("status").cast("string")
        ) \
        .filter(
            (col("order_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("order_amount") > 0)
        ) \
        .dropDuplicates(["order_id"]) \
        .withColumn("processed_date", current_timestamp())
        
        table_path = f"{self.catalog}.{self.silver_schema}.orders"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .partitionBy("order_date") \
            .saveAsTable(table_path)
        
        logger.info(f"Order silver data created: {df.count()} records")
        return df
    
    # =========================================================================
    # GOLD LAYER - Analytics & Aggregations
    # =========================================================================
    
    def create_customer_orders_gold(self):
        """Create customer order aggregations."""
        logger.info("Creating customer orders gold table")
        
        customers = self.spark.table(f"{self.catalog}.{self.silver_schema}.customers")
        orders = self.spark.table(f"{self.catalog}.{self.silver_schema}.orders")
        
        df = customers.join(orders, on="customer_id", how="left") \
            .groupBy("customer_id") \
            .agg(
                col("first_name").first().alias("first_name"),
                col("last_name").first().alias("last_name"),
                col("email").first().alias("email"),
                count("order_id").alias("total_orders"),
                spark_sum("order_amount").alias("total_spent"),
                avg("order_amount").alias("avg_order_value")
            ) \
            .filter(col("total_orders") > 0) \
            .withColumn("processing_date", current_timestamp())
        
        table_path = f"{self.catalog}.{self.gold_schema}.customer_orders"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_path)
        
        logger.info(f"Customer orders gold table created")
    
    def create_product_performance_gold(self):
        """Create product performance metrics."""
        logger.info("Creating product performance gold table")
        
        products = self.spark.table(f"{self.catalog}.{self.silver_schema}.products")
        orders = self.spark.table(f"{self.catalog}.{self.silver_schema}.orders")
        
        df = products.join(orders, on="product_id", how="left") \
            .groupBy("product_id") \
            .agg(
                col("product_name").first().alias("product_name"),
                col("category").first().alias("category"),
                col("price").first().alias("price"),
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_sum("order_amount").alias("total_revenue"),
                count("order_id").alias("total_orders")
            ) \
            .filter(col("total_quantity_sold") > 0) \
            .withColumn("processing_date", current_timestamp())
        
        table_path = f"{self.catalog}.{self.gold_schema}.product_performance"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .partitionBy("category") \
            .saveAsTable(table_path)
        
        logger.info(f"Product performance gold table created")
    
    def create_enriched_orders_gold(self):
        """Create enriched orders table with all details."""
        logger.info("Creating enriched orders gold table")
        
        orders = self.spark.table(f"{self.catalog}.{self.silver_schema}.orders")
        customers = self.spark.table(f"{self.catalog}.{self.silver_schema}.customers")
        products = self.spark.table(f"{self.catalog}.{self.silver_schema}.products")
        
        df = orders \
            .join(
                customers.select(
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
                products.select(
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
                (col("order_amount") / col("quantity")).alias("unit_price")
            ) \
            .withColumn("processing_date", current_timestamp())
        
        table_path = f"{self.catalog}.{self.gold_schema}.enriched_orders"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .partitionBy("order_date") \
            .saveAsTable(table_path)
        
        logger.info(f"Enriched orders gold table created: {df.count()} records")
    
    def create_monthly_revenue_gold(self):
        """Create monthly revenue aggregations."""
        logger.info("Creating monthly revenue gold table")
        
        customers = self.spark.table(f"{self.catalog}.{self.silver_schema}.customers")
        orders = self.spark.table(f"{self.catalog}.{self.silver_schema}.orders")
        
        df = customers.join(orders, on="customer_id", how="inner") \
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
        
        table_path = f"{self.catalog}.{self.gold_schema}.monthly_revenue"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .partitionBy("order_month") \
            .saveAsTable(table_path)
        
        logger.info(f"Monthly revenue gold table created")
    
    def create_top_customers_gold(self, top_n: int = 100):
        """Create top customers ranking."""
        logger.info(f"Creating top {top_n} customers gold table")
        
        customers = self.spark.table(f"{self.catalog}.{self.silver_schema}.customers")
        orders = self.spark.table(f"{self.catalog}.{self.silver_schema}.orders")
        
        customer_spending = customers.join(orders, on="customer_id", how="left") \
            .groupBy("customer_id", "email", "first_name", "last_name") \
            .agg(
                spark_sum("order_amount").alias("total_spent"),
                count("order_id").alias("order_count")
            )
        
        window_spec = Window.orderBy(col("total_spent").desc())
        
        df = customer_spending \
            .withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") <= top_n)
        
        table_path = f"{self.catalog}.{self.gold_schema}.top_customers"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_path)
        
        logger.info(f"Top customers gold table created")
    
    # =========================================================================
    # PIPELINE ORCHESTRATION
    # =========================================================================
    
    def run_full_pipeline(self):
        """Execute complete ETL pipeline end-to-end."""
        logger.info("Starting full data pipeline execution")
        
        try:
            # Create schemas
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.bronze_schema}")
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.silver_schema}")
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.gold_schema}")
            
            # Bronze Layer
            logger.info("=" * 50)
            logger.info("BRONZE LAYER - Ingestion")
            logger.info("=" * 50)
            self.ingest_customers_bronze()
            self.ingest_products_bronze()
            self.ingest_orders_bronze()
            
            # Silver Layer
            logger.info("=" * 50)
            logger.info("SILVER LAYER - Transformation")
            logger.info("=" * 50)
            self.transform_customers_silver()
            self.transform_products_silver()
            self.transform_orders_silver()
            
            # Gold Layer
            logger.info("=" * 50)
            logger.info("GOLD LAYER - Analytics")
            logger.info("=" * 50)
            self.create_customer_orders_gold()
            self.create_product_performance_gold()
            self.create_enriched_orders_gold()
            self.create_monthly_revenue_gold()
            self.create_top_customers_gold()
            
            logger.info("=" * 50)
            logger.info("Pipeline execution completed successfully")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
            raise
    
    def show_pipeline_stats(self):
        """Display pipeline statistics."""
        print("\n" + "=" * 60)
        print("PIPELINE STATISTICS")
        print("=" * 60)
        
        schemas = [
            (self.bronze_schema, "BRONZE"),
            (self.silver_schema, "SILVER"),
            (self.gold_schema, "GOLD")
        ]
        
        for schema, layer_name in schemas:
            print(f"\n{layer_name} LAYER - {schema}:")
            tables = self.spark.sql(f"SHOW TABLES IN {self.catalog}.{schema}").collect()
            for table in tables:
                table_name = table["tableName"]
                count = self.spark.table(f"{self.catalog}.{schema}.{table_name}").count()
                print(f"  - {table_name}: {count:,} records")


def main():
    """Main entry point."""
    # Initialize pipeline
    pipeline = SparkLakeflowPipeline()
    
    # Run full pipeline
    pipeline.run_full_pipeline()
    
    # Show statistics
    pipeline.show_pipeline_stats()


if __name__ == "__main__":
    main()
