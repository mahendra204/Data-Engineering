import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
  name="silver_customers",
  comment="Transformed customers table",
)
def silver_customers():
    df = spark.readStream.table("bronze_customers")

    # Calculate customer age in years
    df = df.withColumn(
        "customer_age",
        when(col("dob").isNotNull(),
             floor(months_between(current_date(), col("dob")) / 12)
        ).otherwise(lit(None))
    )

    # Calculate tenure in days
    df = df.withColumn(
        "tenure_days",
        when(col("join_date").isNotNull(),
             datediff(current_date(), col("join_date"))
        ).otherwise(lit(None))
    )

    # Flag invalid DOBs (before 1900 or after today)
    df = df.withColumn(
        "dob_out_of_range_flag",
        (col("dob") < lit("1900-01-01")) | (col("dob") > current_date())
    )

    # Add transformation timestamp
    df = df.withColumn("transformation_date", current_timestamp())

    return df


@dlt.view (
    name = 'silver_customers_view'
)
def silver_customers_view():
    df = spark.readStream.table('silver_customers')
    return df

@dlt.table(
  name="silver_accounts",
  comment="Transformed accounts_transactions table",
)
def silver_accounts():
  df = spark.readStream.table("bronze_accounts")

  # Categorize channel type
  df = df.withColumn(
      "channel_type",
      when((col("txn_channel") == "ATM") | (col("txn_channel") == "BRANCH"), lit("PHYSICAL"))
      .otherwise(lit("DIGITAL"))
  )

  # Extract year and month from txn_date
  df = df.withColumn("txn_year", year(col("txn_date"))) \
         .withColumn("txn_month", month(col("txn_date")))

  # Extract day of month
  df = df.withColumn("txn_day_dayofmonth", dayofmonth(col("txn_date")))

  # Categorize transaction direction
  df = df.withColumn(
      "txn_direction",
      when(col("txn_type") == "DEBIT", lit("OUT")).otherwise(lit("IN"))
  )
  return df

######

dlt.create_streaming_table(name='silver_customers_transformed_scd1')
dlt.create_auto_cdc_flow(
    target='silver_customers_transformed_scd1',
    source='silver_customers',
    keys=['customer_id'],
    sequence_by='transformation_date',
    stored_as_scd_type=1,
    except_column_list=['transformation_date']
)

# SCD2 - AUTO CDC
dlt.create_streaming_table(name='silver_accounts_transactions_transformed_scd2')
dlt.create_auto_cdc_flow(
    target='silver_accounts_transactions_transformed_scd2',
    source='silver_accounts',
    keys=['txn_id'],
    sequence_by='txn_date',
    except_column_list=['txn_date'],
    stored_as_scd_type=2
)


#########
@dlt.view (
    name = 'silver_accounts_view'
)
def silver_accounts_view():
    df = spark.readStream.table('silver_accounts')
    return df
