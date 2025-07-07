from datetime import datetime, timedelta

from airflow import DAG 

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'mahi', 
    'retries':5, 
    'retry_delay':timedelta(minutes=2)
}

def func():
    print('something is going to happen')

    
with DAG(
    dag_id = 'creating_dagsv2', 
    default_args = default_args,
    description = 'this is my first dag', 
    start_date = datetime(2025, 7, 5, 21), 
    schedule_interval = None
) as dag:
    task1 = BashOperator(
        task_id = 'first_dag', 
        bash_command = 'echo "hello welcome to the world of programming"'
    )
    task2 = BashOperator(
        task_id = 'second_dag', 
        bash_command = 'echo hey, "i am  task2 and will be running after task1"'
    )
    task3 = BashOperator(
        task_id = 'third_task', 
        bash_command = 'echo hey, "i am task3 and will be running after task2"'
    )
    task4 = PythonOperator(
        task_id = 'func', 
        python_callable = func
    )

    task1 >> task3 >> [task2, task4]
