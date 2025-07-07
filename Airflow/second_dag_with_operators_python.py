from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


default_args = {
    'owner': 'mahendra', 
    'retries': 5, 
    'retry_delay': timedelta(minutes = 5)
    }

def greeting():
    print('hello welcome to the world of programming')

def f():
    print('something')

with DAG(
    default_args = default_args, 
    dag_id = 'second_dag_with_python_operator', 
    description = 'write as many dags you want', 
    start_date = datetime(2025, 7, 5), 
    schedule_interval = None
) as dag:
    task1 = PythonOperator(
    task_id = 'greeting', 
    python_callable = greeting
)
    task2 = PythonOperator(
        task_id = 'f', 
        python_callable = f
    )
    
    task1 >> task2