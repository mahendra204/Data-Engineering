"""
common/utils.py - Shared utilities for all Spark jobs
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str, project_id: str) -> SparkSession:
    """Create and return a configured SparkSession."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled",               "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec",      "snappy")
        .config("spark.sql.shuffle.partitions",             "200")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        # BigQuery connector config
        .config("spark.datasource.bigquery.project",        project_id)
        .config("spark.datasource.bigquery.temporaryGcsBucket",
                f"{project_id}-spark-tmp")
        .getOrCreate()
    )


def add_audit_columns(df: DataFrame, execution_date: str) -> DataFrame:
    """Attach standard pipeline audit metadata to every row."""
    return (
        df
        .withColumn("_pipeline_date",       F.lit(execution_date).cast("date"))
        .withColumn("_pipeline_timestamp",  F.current_timestamp())
        .withColumn("_pipeline_version",    F.lit("1.0.0"))
    )


def deduplicate(df: DataFrame, pk_cols: list, order_col: str = None) -> DataFrame:
    """Remove duplicates, keeping the latest row when order_col is given."""
    if order_col:
        from pyspark.sql.window import Window
        w = Window.partitionBy(pk_cols).orderBy(F.col(order_col).desc())
        return (
            df
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
    return df.dropDuplicates(pk_cols)


def validate_not_empty(df: DataFrame, label: str):
    """Raise if DataFrame has zero rows."""
    count = df.count()
    if count == 0:
        raise ValueError(f"Validation failed: {label} has 0 rows.")
    logger.info(f"Validation passed: {label} has {count:,} rows.")


def validate_no_nulls(df: DataFrame, cols: list, label: str):
    """Raise if any of the specified columns contain nulls."""
    for col in cols:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise ValueError(
                f"Validation failed: {label}.{col} has {null_count} null values."
            )
    logger.info(f"Null validation passed for {label}: {cols}")


def write_to_gcs(
    df: DataFrame,
    output_path: str,
    partition_cols: list = None,
    mode: str = "overwrite",
    format: str = "parquet",
):
    """Write DataFrame to GCS in Parquet format with optional partitioning."""
    writer = df.write.mode(mode).format(format)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(output_path)
    logger.info(f"Wrote {format} to {output_path}")


def write_to_bigquery(
    df: DataFrame,
    bq_table: str,
    project_id: str,
    mode: str = "append",
    partition_field: str = None,
):
    """Write DataFrame to BigQuery using the Spark BigQuery connector."""
    writer = (
        df.write
        .format("bigquery")
        .option("table",      bq_table)
        .option("project",    project_id)
        .mode(mode)
    )
    if partition_field:
        writer = writer.option("partitionField", partition_field)

    writer.save()
    logger.info(f"Wrote to BigQuery table: {bq_table}")


def read_from_bigquery(spark: SparkSession, query: str, project_id: str) -> DataFrame:
    """Read from BigQuery using a SQL query."""
    return (
        spark.read
        .format("bigquery")
        .option("project",    project_id)
        .option("query",      query)
        .load()
    )
