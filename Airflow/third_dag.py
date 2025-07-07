from airflow import DAG

from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

dag = DAG(
    'extract_load_data',
    start_date=datetime(2025, 7, 1),  # or any past date you want
    schedule_interval=None,       # optional: define how often it runs
    catchup=False                     # optional: skip backfill if not needed
)

def extract_data():
    # Extract data from the database
    pass

def load_data():
    # Load data into the data warehouse
    pass


extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

extract_data_task >> load_data_task