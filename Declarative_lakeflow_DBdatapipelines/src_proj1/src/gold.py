from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

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
