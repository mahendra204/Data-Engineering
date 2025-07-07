from datetime import datetime as dtime
from datetime import timedelta
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

import pandas as pd 


GV = "Data Engineering"

def extract_fn():
    print("Logic to Extract data")
    print("Value of Global Variable is: ", GV)
    rtn_val = "Analytics Training"
    return rtn_val

# XCOM: Cross = Communication
# ti stands for task instance
def transform_fn(a1, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids=["EXTRACT"])
    print("type of xcom pull object is {}".format(type(xcom_pull_obj)))
    extract_rtn_obj = xcom_pull_obj[0]
    print(f"the value of xcom pull object is {extract_rtn_obj}")
    print("The value of a1 is ", a1)
    print("Logic to Transform Data")
    return 10

def load_fn(p1, p2, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids=["EXTRACT"])
    print('type of xcom pull object is {}'.format(type(xcom_pull_obj)))
    print("the value of xcom pull object is {}".format(xcom_pull_obj[0]))
    print("The value of p1 is {}".format(p1))
    print("The value of p2 is {}".format(p2))
    print("Logic to load Data")

default_args = {
    "owner": "Airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": dtime(2024, 1, 30)
}

with DAG("ex_xcom_push_pull",
        default_args=default_args, 
        catchup=False) as dag:
    
    start = DummyOperator(task_id="START")

    e = PythonOperator(
        task_id="EXTRACT",
        python_callable=extract_fn,
        do_xcom_push=True  # By default this parameter is set to True
    )

    t = PythonOperator(
        task_id="TRANSFORM",
        python_callable=transform_fn,
        op_args=["Learning Data Engineering with airflow"],
        do_xcom_push=True
    )

    # op_args -> Operator Arguments
    l = PythonOperator(
        task_id="LOAD",
        python_callable=load_fn,
        op_args={"p2": "Engineering", "p1": "Data"}
    ) 

    end = DummyOperator(task_id="END")

start >> e >> t >> l >> end