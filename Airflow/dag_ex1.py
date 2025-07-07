from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import TaskInstance
import random

def branch_decision(**args):
    """Randomly choose between success (True) and failure (False)."""
    return 'success_task' if random.choice([True, False]) else 'failure_task'

def notify_outcome(ti: TaskInstance, **args):
    """Log the notification message using the default logger."""
    outcome = ti.xcom_pull(task_ids='branch_decision')
    if outcome == 'success_task':
        notification_message = 'Success: The operation was successful.'
    else:
        notification_message = 'Failure: The operation failed.'
    ti.log.info(notification_message)
    return notification_message

# PART 1. Definition of the DAG
with DAG(
    dag_id='data_analysis_flow',
    start_date=datetime(2024,3,5),
    default_args={
        'owner': 'airflow',
    },
    description='A DAG for data analysis with dynamic notification based on outcome',
    schedule_interval="@daily",
    catchup=False,
    tags=['data-analysis'],
) as dag:

    # PART 2. Definition of the tasks
    start = DummyOperator(task_id='start')

    data_analysis = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=branch_decision,
    )

    success_task = BashOperator(
        task_id='success_task',
        bash_command='echo "Operation succeeded."',
    )

    failure_task = BashOperator(
        task_id='failure_task',
        bash_command='echo "Operation failed." && exit 1',
    )

    notification_task = PythonOperator(
        task_id='notification_task',
        python_callable=notify_outcome,
        trigger_rule=TriggerRule.DUMMY
    )

    # PART 3. Definition of the Workflow
    start >> data_analysis >> [success_task, failure_task]
    [success_task, failure_task] >> notification_task