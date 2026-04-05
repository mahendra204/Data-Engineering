"""
Spark Transform Job - Generic Table Transformer
Reads raw Parquet from GCS, applies transformations, writes processed Parquet back to GCS.
Also writes directly to BigQuery via Spark BigQuery connector.
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType
from utils import (
    get_spark_session,
    add_audit_columns,
    deduplicate,
    validate_not_empty,
    write_to_gcs,
    write_to_bigquery,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Generic Spark ETL Transform")
    parser.add_argument("--input_path",     required=True)
    parser.add_argument("--output_path",    required=True)
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--project_id",     required=True)
    parser.add_argument("--bq_dataset",     required=True)
    parser.add_argument("--table_name",     required=True)
    return parser.parse_args()


# ─── Table-specific transform registry ──────────────────────────────────────────
def transform_orders(df, execution_date):
    return (
        df
        .filter(F.col("status").isin(["completed", "pending", "cancelled"]))
        .withColumn("order_value_usd",
                    F.round(F.col("amount") / F.col("exchange_rate"), 2))
        .withColumn("is_high_value",
                    F.when(F.col("order_value_usd") > 1000, True).otherwise(False))
        .withColumn("order_year",  F.year("created_at"))
        .withColumn("order_month", F.month("created_at"))
        .withColumn("order_day",   F.dayofmonth("created_at"))
        .dropDuplicates(["order_id"])
    )


def transform_customers(df, execution_date):
    return (
        df
        .withColumn("full_name",
                    F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        .withColumn("email_domain",
                    F.regexp_extract(F.col("email"), r"@(.+)$", 1))
        .withColumn("customer_age_days",
                    F.datediff(F.lit(execution_date), F.col("created_at").cast("date")))
        .withColumn("is_active",
                    F.col("status") == "active")
        .drop("password_hash", "ssn", "credit_card_number")   # PII removal
        .dropDuplicates(["customer_id"])
    )


def transform_products(df, execution_date):
    return (
        df
        .filter(F.col("is_deleted") == False)
        .withColumn("discount_pct",
                    F.round((1 - F.col("sale_price") / F.col("list_price")) * 100, 2))
        .withColumn("category_path",
                    F.concat_ws("/", F.col("category_l1"), F.col("category_l2"), F.col("category_l3")))
        .withColumn("in_stock", F.col("inventory_qty") > 0)
        .dropDuplicates(["product_id"])
    )


def transform_payments(df, execution_date):
    return (
        df
        .filter(F.col("status") != "voided")
        .withColumn("payment_method_group",
                    F.when(F.col("payment_method").isin(["visa", "mastercard", "amex"]), "card")
                     .when(F.col("payment_method").isin(["paypal", "stripe"]), "wallet")
                     .otherwise("other"))
        .withColumn("is_refunded", F.col("refund_amount") > 0)
        .withColumn("net_amount",
                    F.col("amount") - F.coalesce(F.col("refund_amount"), F.lit(0)))
        .dropDuplicates(["payment_id"])
    )


TRANSFORMS = {
    "orders":    transform_orders,
    "customers": transform_customers,
    "products":  transform_products,
    "payments":  transform_payments,
}


def main():
    args = parse_args()
    logger.info(f"Starting transform for table={args.table_name} date={args.execution_date}")

    spark = get_spark_session(
        app_name=f"ETL-Transform-{args.table_name}",
        project_id=args.project_id,
    )

    # ── Read raw data ────────────────────────────────────────────────────────────
    logger.info(f"Reading raw data from {args.input_path}")
    df_raw = spark.read.parquet(args.input_path)
    logger.info(f"Raw record count: {df_raw.count()}")

    validate_not_empty(df_raw, f"Raw {args.table_name}")

    # ── Apply table-specific transform ───────────────────────────────────────────
    transform_fn = TRANSFORMS.get(args.table_name)
    if not transform_fn:
        raise ValueError(f"No transform registered for table: {args.table_name}")

    df_transformed = transform_fn(df_raw, args.execution_date)

    # ── Add standard audit columns ───────────────────────────────────────────────
    df_final = add_audit_columns(df_transformed, args.execution_date)

    record_count = df_final.count()
    logger.info(f"Transformed record count: {record_count}")

    # ── Write to GCS (processed zone) ───────────────────────────────────────────
    write_to_gcs(df_final, args.output_path, partition_cols=["_pipeline_date"])
    logger.info(f"Written to GCS: {args.output_path}")

    # ── Write to BigQuery ────────────────────────────────────────────────────────
    bq_table = f"{args.project_id}.{args.bq_dataset}.{args.table_name}"
    write_to_bigquery(df_final, bq_table, args.project_id)
    logger.info(f"Written to BigQuery: {bq_table}")

    spark.stop()
    logger.info("Transform complete.")


if __name__ == "__main__":
    main()
