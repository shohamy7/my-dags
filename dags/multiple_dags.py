from datetime import datetime, timedelta
from airflow import DAG
from kubernetes import client as k8s

from airflow.operators.python import PythonOperator

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def print_greetings(ti, **kwargs):
    import os
    greetings = os.getenv('GREETING', "No greeting")
    print(f"kwargs: {kwargs}")
    print(f"ti: {ti}")
    print(f"try number: {ti.try_number}")
    print(f"Greeting: {greetings}")


for i in range(2):
    # Define the DAG object with the specified parameters
    with DAG(
        dag_id=f'multiple_dag_{i}',
        default_args=default_args,
        schedule_interval="* * * * *",  # This DAG is triggered manually
        catchup=False,
    ) as dag:
        
        # Define the BashOperator task that prints the greeting to stdout
        print_greeting_task = PythonOperator(
            task_id=f'print_greeting_{i}',
            python_callable=print_greetings
        )
