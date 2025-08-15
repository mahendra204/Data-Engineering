from google.cloud import bigquery
#connection details
project_id='project1x-467415'
dataset_id='meanlifestudies'
table_id='employees'
bucket_name='mybucka'
gcs_file_path='output/employees.csv'

def load_gcs_bucketdata_to_bigquery():
    try:
        client = bigquery.Client(project=project_id)
        bq_table = client.dataset(dataset_id).table(table_id)
        file_path_URL = f"gs://{bucket_name}/{gcs_file_path}"

        job_details = bigquery.LoadJobConfig(
            source_format='CSV',
            skip_leading_rows=1, #skipping the header row
            autodetect=True, #automatically this will detect the schema of our input data
            write_disposition= "write_truncate" #each data loading this will truncate and loads the data
            #we can also other options as write_append to append the data for each load. 
        )

        data_load = client.load_table_from_uri(file_path_URL, bq_table, job_config=job_details)
        data_load.result()  # this ensures the data loades successfully and it Wait for job to be completed
        print(f"Loaded {data_load.output_rows} rows into {dataset_id}.{table_id}")
    except Exception as e:
        print("BigQuery load failed. Details:", e)

load_gcs_bucketdata_to_bigquery()
