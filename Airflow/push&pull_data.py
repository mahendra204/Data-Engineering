from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mahendra',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greeting(ti):
    first_name = ti.xcom_pull(task_ids = 'get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids = 'get_name', key='last_name')
    age = ti.xcom_pull(task_ids ='get_age', key = 'age')
    print(f'hello {first_name} welcome to the world of programming I am {age} years old,is your last_name is {last_name}?')


def f(x):
    sums = 0
    for i in range(x + 1):
        sums += i
    return sums

def get_age(ti):
    age = ti.xcom_push(key = 'age', value = 55)
    
def get_name(ti):
    ti.xcom_push(key='first_name', value='kamireddy')
    ti.xcom_push(key='last_name', value='mahendra')

with DAG(
    default_args=default_args,
    dag_id='push_pull_with_python',
    description='write as many dags you want',
    start_date=datetime(2025, 7, 5),
    schedule_interval=None
) as dag:

    task1 = PythonOperator(
        task_id='greeting',
        python_callable=greeting
    )

    task2 = PythonOperator(
        task_id='f',
        python_callable=f,
        op_kwargs={'x':40}
    )

    task3 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )
    task4 = PythonOperator(
        task_id = 'get_age', 
        python_callable = get_age
    )

    [task3, task4] >> task1 >> task2  # DAG flow: get_name → greeting → f
