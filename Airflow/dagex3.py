from airflow import DAG 
from airflow.operators.dummy import DummyOperator
from datetime import datetime as dt

def_args = {
    "owner": "airflow",
    "start_date": dt(2024, 1, 1),
    'retries':5, 
    'retry_delay':5
}

with DAG(
    dag_id = "ETL",
    catchup=False,
    default_args=def_args, 
    schedule_interval = '@daily'
) as dag:
        start = DummyOperator(task_id="START")
        e = DummyOperator(task_id="EXTRACTION")
        t = DummyOperator(task_id="TRANSFORMATION")
        l = DummyOperator(task_id="LOADING")
        end = DummyOperator(task_id="END")

start >> e >> t >> l >> end