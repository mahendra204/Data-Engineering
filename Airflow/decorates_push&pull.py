from airflow.decorators import dag,task

from datetime import datetime, timedelta


default_args = {
    'owner': 'mahi', 
    'retries': 5, 
    'retry_delay':timedelta(minutes = 5)
}

@dag(dag_id = 'dag_with_taskflow_api3', 
     default_args = default_args, 
     start_date = datetime(2025, 7, 1),
     schedule_interval = None)
def hello_world_ETL():
    
    @task(multiple_outputs = True)
    def get_name():
        return {
            'first_name' : 'mahendra', 
            'last_name' : 'kamireddy'
        }
    @task()
    def get_age():
        return 88
    
    @task()
    def greeting(first_name,last_name, age):
        print(f'hello welcome to airflow, my first name is {first_name}, last_name is {last_name} and my age is {age}')

    name_dict = get_name()
    age = get_age()
    greeting(first_name= name_dict['first_name'],
             last_name = name_dict['last_name'],
             age=age)
greet_dag = hello_world_ETL()

    