from airflow.decorators import dag,task

from datetime import datetime, timedelta


default_args = {
    'owner': 'mahi', 
    'retries': 5, 
    'retry_delay':timedelta(minutes = 5)
}

@dag(dag_id = 'dag_with_taskflow_api', 
     default_args = default_args, 
     start_date = datetime(2025, 7, 1),
     schedule_interval = None)
def hello_world_ETL():
    
    @task()
    def get_name():
        return 'mahi'
    @task()
    def get_age():
        return 88
    
    @task()
    def greeting(name, age):
        print(f'hello welcome to airflow, my name is {name} and my age is {age}')

    name = get_name()
    age = get_age()
    greeting(name=name, age=age)
greet_dag = hello_world_ETL()

    