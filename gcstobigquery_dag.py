from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

#Define your DAG
default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}
with DAG(
    dag_id='gcs_to_bigquery_dag_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    #Task to list objects in the GCS bucket with a specific prefix
    list_gcs_objects = GCSListObjectsOperator(
        task_id='list_gcs_objects',
        bucket='your-bucket',
        prefix='Daily_Port_Activity_Data_and_Trade_Estimates',
        delimiter='/',
        gcp_conn_id='google_cloud_default',
    )
    #Task to load all CSV files from GCS bucket to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='your-bucket',
        source_objects=list_gcs_objects.output,
        destination_project_dataset_table='project-id.data.daily_port_data',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )
    #Task to run a BigQuery query and insert the result into another table
    run_bq_query = BigQueryInsertJobOperator(
        task_id='run_bq_query',
        configuration={
            "query": {
                "query": """
                    SELECT *
                    FROM `project-id.data.daily_port_data`
                    WHERE portname IN ('Klaipeda')
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "project-id",
                    "datasetId": "data",
                    "tableId": "port579_data"
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        gcp_conn_id='bigquery_default',
        location='US',
    )
    #Define task dependencies
    list_gcs_objects >> load_to_bigquery >> run_bq_query