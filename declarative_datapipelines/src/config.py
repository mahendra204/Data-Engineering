\"\"\"\nDatabricks Delta Live Tables Pipeline Configuration
Complete configuration for end-to-end declarative pipelines
\"\"\"

import os
from typing import Dict, List

# ============================================================================
# PIPELINE CONFIGURATION
# ============================================================================

PIPELINE_NAME = "ecommerce_declarative_pipeline"
PIPELINE_VERSION = "2.0.0"
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

# ============================================================================
# CATALOG & SCHEMA CONFIGURATION
# ============================================================================

CATALOG_NAME = "main"
SCHEMA_BRONZE = "bronze_ecommerce"
SCHEMA_SILVER = "silver_ecommerce"
SCHEMA_GOLD = "gold_ecommerce"

# Fully qualified schema names
BRONZE_SCHEMA_FQ = f"{CATALOG_NAME}.{SCHEMA_BRONZE}"
SILVER_SCHEMA_FQ = f"{CATALOG_NAME}.{SCHEMA_SILVER}"
GOLD_SCHEMA_FQ = f"{CATALOG_NAME}.{SCHEMA_GOLD}"

# ============================================================================
# DATA PATHS
# ============================================================================

SOURCE_DATA_PATH = "/dbfs/data"
BRONZE_PATH = f"/user/hive/warehouse/{SCHEMA_BRONZE}"
SILVER_PATH = f"/user/hive/warehouse/{SCHEMA_SILVER}"
GOLD_PATH = f"/user/hive/warehouse/{SCHEMA_GOLD}"

# ============================================================================
# TABLE CONFIGURATIONS
# ============================================================================

BRONZE_TABLES = ["customers", "products", "orders"]
SILVER_TABLES = ["customers", "products", "orders"]
GOLD_TABLES = [
    "customer_orders",
    "product_performance",
    "monthly_customer_revenue",
    "category_summary",
    "top_customers",
    "enriched_orders"
]

# ============================================================================
# DATA QUALITY EXPECTATIONS
# ============================================================================

QUALITY_EXPECTATIONS: Dict[str, Dict] = {
    "bronze_customers": {
        "min_records": 1,
        "required_columns": ["customer_id", "email", "first_name", "last_name", "phone"]
    },
    "bronze_products": {
        "min_records": 1,
        "required_columns": ["product_id", "product_name", "category", "price", "stock_quantity"]
    },
    "bronze_orders": {
        "min_records": 1,
        "required_columns": ["order_id", "customer_id", "product_id", "order_date", "order_amount"]
    },
    "silver_customers": {
        "min_records": 1,
        "required_columns": ["customer_id", "email", "registration_date"]
    },
    "silver_orders": {
        "min_records": 1,
        "required_columns": ["order_id", "customer_id", "order_amount"]
    },
    "gold_customer_orders": {
        "min_records": 0,
        "required_columns": ["customer_id", "total_orders", "total_spent"]
    }
}

# ============================================================================
# PARTITION STRATEGY
# ============================================================================

PARTITION_COLUMNS: Dict[str, str] = {
    "orders": "order_date",
    "monthly_revenue": "order_month",
    "customers": "country",
    "products": "category",
    "enriched_orders": "order_date"
}

# ============================================================================
# REFRESH & PERFORMANCE CONFIGURATION
# ============================================================================

REFRESH_INTERVAL = "1 hour"  # DLT refresh interval
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 60

# Spark configuration for optimization
SPARK_CONFIG = {
    "spark.databricks.delta.schema.autoMigrate.enabled": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true"
}

# ============================================================================
# LOGGING & MONITORING
# ============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
ENABLE_METRICS = True
METRICS_PATH = f"/user/hive/warehouse/metrics/{SCHEMA_BRONZE}"

# Notification Configuration
ALERT_EMAIL = "data-team@example.com"
ENABLE_ALERTS = True

# Performance Configuration
AUTO_SCALE_MIN_WORKERS = 2
AUTO_SCALE_MAX_WORKERS = 8
WORKER_TYPE = "i3.xlarge"
DRIVER_NODE_TYPE = "i3.xlarge"
