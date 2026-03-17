from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import dlt

@dlt.table(
    name = 'gold_cust_acc'
)

def gold_cust_agg():
    cust = dlt.read('silver_customers')
    acc = dlt.read('silver_accounts')
    joined =  cust.join(acc, on = 'customer_id', how = 'inner')
    return joined

@dlt.table(
    name = 'gold_cust_acc_agg'
)
def gold_cust_acc_agg():
    df = dlt.read('gold_cust_acc')
    agg_df = df.groupBy(
        "customer_id",
        "name",
        "gender",
        "city",
        "status",
        "income_range"
        # "risk_segment",
        # "customer_age",
        # "tenure_days"
    ).agg(
        F.countDistinct("account_id").alias("accounts_count"),
        F.count("*").alias("txn_count"),
        F.sum(
            F.when(F.col("txn_type") == "CREDIT", F.col("txn_amount"))
            .otherwise(F.lit(0.0))
        ).alias("total_credits"),
        F.sum(
            F.when(F.col("txn_type") == "DEBIT", F.col("txn_amount"))
            .otherwise(F.lit(0.0))
        ).alias("total_debits")
        # F.avg(F.col("txn_amount")).alias("avg_txn_amount"),
        # F.min("txn_date").alias("first_txn_date"),
        # F.max("txn_date").alias("last_txn_date"),
        # F.countDistinct("txn_channel").alias("channels_used")
    )
    return agg_df
                                            