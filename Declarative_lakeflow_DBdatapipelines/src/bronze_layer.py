import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = 'bronze_customers'
)
@dlt.expect_or_drop("valid_customer_name", "name IS NOT NULL")
@dlt.expect_or_drop("valid_dob", "dob IS NOT NULL")
@dlt.expect_or_drop("valid_city", "city IS NOT NULL")
# @dlt.expect_or_drop("valid_join_date", "join_date IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL")
@dlt.expect_or_drop("valid_phone", "phone_number IS NOT NULL")
# @dlt.expect_or_drop("valid_channel", "preferred_channel IS NOT NULL")
@dlt.expect_or_drop("valid_occupation", "occupation IS NOT NULL")
@dlt.expect_or_drop("valid_income", "income_range IS NOT NULL")
@dlt.expect_or_drop("valid_risk_segment", "risk_segment IS NOT NULL")
@dlt.expect_or_drop("valid_gender", "gender IS NOT NULL")
# @dlt.expect_or_drop("valid_status", "status IS NOT NULL")
def fun():
    df = dlt.read_stream("land_customers")
    df = df.withColumn("name", upper(col("name")))
    df = df.withColumn("email", upper(col("email")))
    df = df.withColumn("occupation", upper(col("occupation")))
    df = df.withColumn("city", upper(col("city")))
    df = df.withColumn("income_range", upper(col("income_range")))
    df = df.withColumn("risk_segment", upper(col("risk_segment")))
    df = df.withColumn('channel', upper(col('preferred_channel')))

    # Convert dob to date format
    df = df.withColumn("dob", to_date(col("dob"), "dd/MM/yyyy"))

    # Convert join_date to date format
    df = df.withColumn(
        "gender",
        when(col("gender") == "M", "MALE")
        .when(col("gender") == "F", "FEMALE")
        .otherwise("UNKNOWN")
    )

    # Replace null or empty status values with "UNKNOWN"
    df = df.withColumn(
        'status',
        when(col('status').isNull() | (trim(col('status')) == ""), lit("UNKNOWN"))
        .otherwise(col('status'))
    )

    # Trim whitespace from phone_number
    df = df.withColumn('phone_number', trim(col('phone_number')))

    # Remove all non-digit characters from phone_number
    df = df.withColumn('phone_number', regexp_replace(col('phone_number'), r"[^0-9]", ""))

    # Keep only rows where phone_number matches UK format (starts with 44 + 10 digits)
    df = df.filter(col('phone_number').rlike("^44\\d{10}$"))


# Filter rows where preferred_channel is valid
    df = df.filter(col("preferred_channel").isin("ONLINE", "MOBILE", "BRANCH", "ATM"))

    # Filter rows where income_range is valid
    df = df.filter(col("income_range").isin("HIGH", "MEDIUM", "LOW", "VERY HIGH"))

    # Filter rows where risk_segment is valid
    df = df.filter(col("risk_segment").isin("LOW", "MEDIUM", "HIGH", "UNKNOWN"))
    return df


@dlt.table(
  name="bronze_accounts",
  comment="This table contains the cleaned data from the transactions ingestion"
)
@dlt.expect_or_fail("valid_account_id", "account_id IS NOT NULL")
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_txn_id", "txn_id IS NOT NULL")
@dlt.expect_or_drop("account_type", "account_type IS NOT NULL")
@dlt.expect_or_drop("valid_balance", "balance IS NOT NULL")
@dlt.expect_or_drop("valid_txn_date", "txn_date IS NOT NULL")
@dlt.expect_or_drop("valid_txn_amount", "txn_amount IS NOT NULL")
@dlt.expect_or_drop("valid_txn_type", "txn_type IS NOT NULL")
@dlt.expect_or_drop("valid_txn_channel", "txn_channel IS NOT NULL")
def bronze_accounts():
    df = dlt.read_stream("land_accounts")
    #### cleaning
    # Standardize columns to uppercase
    df = df.withColumn("account_type", upper(col("account_type")))
    df = df.withColumn("txn_channel", upper(col("txn_channel")))
    df = df.withColumn("txn_type", upper(col("txn_type")))

    # Map transaction types (fixing logical redundancy)
    df = df.withColumn(
    "txn_type",
    when(col("txn_type") == "DEBITT", "DEBIT")
    .when(col("txn_type") == "CREDITT", "CREDIT")
    .otherwise(col("txn_type"))
)


    return df

