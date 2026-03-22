"""
data_quality.py - Post-load data quality checks on BigQuery tables
Runs after all tables are loaded. Fails the DAG step if checks don't pass.
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from common.utils import get_spark_session, read_from_bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHECKS = {
    "orders": [
        ("row_count",          "SELECT COUNT(*) as cnt FROM `{table}` WHERE DATE(created_at)='{date}'", 1),
        ("no_null_order_id",   "SELECT COUNT(*) as cnt FROM `{table}` WHERE order_id IS NULL AND DATE(created_at)='{date}'", 0),
        ("no_negative_amount", "SELECT COUNT(*) as cnt FROM `{table}` WHERE amount < 0 AND DATE(created_at)='{date}'", 0),
    ],
    "customers": [
        ("row_count",          "SELECT COUNT(*) as cnt FROM `{table}` WHERE DATE(updated_at)='{date}'", 1),
        ("unique_emails",      "SELECT COUNT(*)-COUNT(DISTINCT email) as cnt FROM `{table}` WHERE DATE(updated_at)='{date}'", 0),
    ],
    "products": [
        ("row_count",          "SELECT COUNT(*) as cnt FROM `{table}` WHERE DATE(updated_at)='{date}'", 1),
    ],
    "payments": [
        ("row_count",          "SELECT COUNT(*) as cnt FROM `{table}` WHERE DATE(payment_date)='{date}'", 1),
        ("positive_net_amount","SELECT COUNT(*) as cnt FROM `{table}` WHERE net_amount < 0 AND DATE(payment_date)='{date}'", 0),
    ],
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id",     required=True)
    parser.add_argument("--bq_dataset",     required=True)
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--tables",         required=True, help="comma-separated table names")
    return parser.parse_args()


def run_check(spark, project_id, bq_dataset, table_name, check_name, query_tpl, expected, execution_date):
    full_table = f"{project_id}.{bq_dataset}.{table_name}"
    query = query_tpl.format(table=full_table, date=execution_date)
    try:
        df = read_from_bigquery(spark, query, project_id)
        result = df.collect()[0]["cnt"]
        # For row_count checks: result must be >= expected; others must equal expected
        passed = (result >= expected) if check_name == "row_count" else (result == expected)
        status = "PASS" if passed else "FAIL"
        logger.info(f"[{status}] {table_name}.{check_name}: result={result}, expected={expected}")
        return passed
    except Exception as e:
        logger.error(f"[ERROR] {table_name}.{check_name}: {e}")
        return False


def main():
    args = parse_args()
    spark = get_spark_session("DataQualityChecks", args.project_id)

    tables = [t.strip() for t in args.tables.split(",")]
    all_passed = True

    for table in tables:
        checks = CHECKS.get(table, [])
        for check_name, query_tpl, expected in checks:
            passed = run_check(
                spark, args.project_id, args.bq_dataset,
                table, check_name, query_tpl, expected, args.execution_date
            )
            if not passed:
                all_passed = False

    spark.stop()

    if not all_passed:
        logger.error("One or more data quality checks FAILED. Failing the job.")
        sys.exit(1)

    logger.info("All data quality checks PASSED.")


if __name__ == "__main__":
    main()
