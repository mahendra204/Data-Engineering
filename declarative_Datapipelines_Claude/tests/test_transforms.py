"""
tests/test_transforms.py — Unit tests for Spark transform logic
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date

# Import transform functions
import sys
sys.path.insert(0, "spark_jobs")
from transform_generic import transform_orders, transform_customers, transform_products, transform_payments
from common.utils import add_audit_columns, deduplicate, validate_not_empty


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ─── Orders ─────────────────────────────────────────────────────────────────────
def test_transform_orders_filters_status(spark):
    data = [
        ("o1", "c1", "completed", 100.0, 1.0, "2024-01-15 10:00:00"),
        ("o2", "c2", "fraud",     200.0, 1.0, "2024-01-15 10:00:00"),
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "status", "amount", "exchange_rate", "created_at"])
    df = df.withColumn("created_at", F.to_timestamp("created_at"))
    result = transform_orders(df, "2024-01-15")
    assert result.count() == 1
    assert result.first()["order_id"] == "o1"


def test_transform_orders_high_value(spark):
    data = [("o1", "c1", "completed", 1500.0, 1.0, "2024-01-15 10:00:00")]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "status", "amount", "exchange_rate", "created_at"])
    df = df.withColumn("created_at", F.to_timestamp("created_at"))
    result = transform_orders(df, "2024-01-15")
    assert result.first()["is_high_value"] == True


def test_transform_orders_deduplicates(spark):
    data = [
        ("o1", "c1", "completed", 100.0, 1.0, "2024-01-15 10:00:00"),
        ("o1", "c1", "completed", 100.0, 1.0, "2024-01-15 10:00:00"),
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "status", "amount", "exchange_rate", "created_at"])
    df = df.withColumn("created_at", F.to_timestamp("created_at"))
    result = transform_orders(df, "2024-01-15")
    assert result.count() == 1


# ─── Customers ──────────────────────────────────────────────────────────────────
def test_transform_customers_pii_removal(spark):
    data = [("c1", "John", "Doe", "john@example.com", "active", "2024-01-01 00:00:00", "2024-01-15 00:00:00", "SECRET", "1234")]
    df = spark.createDataFrame(data,
        ["customer_id", "first_name", "last_name", "email", "status", "created_at", "updated_at", "password_hash", "ssn"])
    df = df.withColumn("created_at", F.to_timestamp("created_at")).withColumn("updated_at", F.to_timestamp("updated_at"))
    result = transform_customers(df, "2024-01-15")
    assert "password_hash" not in result.columns
    assert "ssn" not in result.columns


def test_transform_customers_full_name(spark):
    data = [("c1", "Jane", "Smith", "jane@test.com", "active", "2024-01-01 00:00:00", "2024-01-15 00:00:00")]
    df = spark.createDataFrame(data,
        ["customer_id", "first_name", "last_name", "email", "status", "created_at", "updated_at"])
    df = df.withColumn("created_at", F.to_timestamp("created_at")).withColumn("updated_at", F.to_timestamp("updated_at"))
    result = transform_customers(df, "2024-01-15")
    assert result.first()["full_name"] == "Jane Smith"
    assert result.first()["email_domain"] == "test.com"


# ─── Utils ──────────────────────────────────────────────────────────────────────
def test_add_audit_columns(spark):
    df = spark.createDataFrame([("a",)], ["col1"])
    result = add_audit_columns(df, "2024-01-15")
    assert "_pipeline_date" in result.columns
    assert "_pipeline_timestamp" in result.columns
    assert "_pipeline_version" in result.columns


def test_validate_not_empty_raises(spark):
    df = spark.createDataFrame([], spark.createDataFrame([("a",)], ["col1"]).schema)
    with pytest.raises(ValueError, match="0 rows"):
        validate_not_empty(df, "test_table")


def test_deduplicate(spark):
    data = [("id1", "2024-01-15"), ("id1", "2024-01-14"), ("id2", "2024-01-15")]
    df = spark.createDataFrame(data, ["id", "updated_at"])
    result = deduplicate(df, ["id"], order_col="updated_at")
    assert result.count() == 2
