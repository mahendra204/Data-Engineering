cust_file_path = '/Volumes/myfirst_project_catalog/myfirst_project_schema/myfirst_project_volume/customers/'
acc_file_path = '/Volumes/myfirst_project_catalog/myfirst_project_schema/myfirst_project_volume/accounts/'

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), True),
    StructField("join_date", DateType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("preferred_channel", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("income_range", StringType(), True),
    StructField("risk_segment", StringType(), True)
])

@dlt.table(
    name = 'land_customers'
)
def fun():
    df = spark.readStream.format('cloudFiles') \
        .option('cloudFiles.format', 'csv') \
        .option('header', 'true') \
        .schema(customer_schema) \
        .load(cust_file_path)
    return df

acc_schema = StructType([
    StructField("account_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("txn_id", LongType(), True),
    StructField("txn_date", DateType(), True),
    StructField("txn_type", StringType(), True),
    StructField("txn_amount", DoubleType(), True),
    StructField("txn_channel", StringType(), True)
])

@dlt.table(
    name = 'land_accounts'
)
def fun():
    df = spark.readStream.format('cloudFiles') \
        .option('cloudFiles.format', 'csv') \
        .option('header', 'true') \
        .schema(acc_schema) \
        .load(acc_file_path)
    return df